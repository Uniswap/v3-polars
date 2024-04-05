from .helpers import *
import polars as pl
from pathlib import Path
PACKAGEDIR = Path(__file__).parent.absolute()

class v3Pool:
    def __init__(
        self,
        pool,
        chain,
        update_from = 'gcp',
        low_memory=False,
        update=False,
        pull=True,
        tgt_max_rows=1_000_000,
    ):
        """
        Impliments and maintains a representation of Uniswap v3 Pool

        Supported features
        1. Maintain mints/burns/initalizes/factories/swaps
        on all required chains
        2. Creates helpers for reading Uniswap v3 data
        3. Impliments Optimism OVM1 -> EVM migration seemlessly
        4. Allows swap simulating
        5. Creates liquidity distributions
        6. Historical price helpers
        """
        # uniswap v3 immutables
        self._Q96 = 2**96
        self.MAX_TICK = 887272

        # data adjustments
        self.tgt_max_rows = tgt_max_rows
        self.pull = pull
        self.low_memory = low_memory

        # specific v3 pool/chain data
        self.chain = chain
        # remove checksums
        self.pool = pool.lower()
        
        # this is the cache where we store data if needed
        self.cache = {}
        self.cache["as_of"] = 0

        # data checkers
        self.path = f"{PACKAGEDIR}/data"
        self.data_path = f"{PACKAGEDIR}/data"
        checkPath("", self.data_path)

        # we strip the "uniswap_v3_factory"
        # and "uniswap_v3_pool" bc its unneeded
        self.tables = [
            "uniswap_v3_factory_pool_created_events_combined",
            "uniswap_v3_pool_swap_events_combined",
            "uniswap_v3_pool_mint_burn_events_combined",
            "uniswap_v3_pool_initialize_events_combined",
        ]

        # data quality assurances
        self.max_supported = -1

        if update:
            update_tables(self, update_from, self.tables)

            if self.chain == "optimism":
                print("Chain = optimism - Start pulling the ovm1")
                # the ovm1 = optimism, but they are in seperate databases
                # pain
                try:
                    self.chain = "optimism_legacy_ovm1"
                    update_tables(self, update_from, self.tables)
                except Exception as e:
                    raise (e)
                # we dont want these race condition
                finally:
                    self.chain = "optimism"

        
        self.ts, self.fee, self.token0, self.token1 = initializePoolFromFactory(
            self.pool, self.chain, self.data_path
        )

        if pull:
            self.readFromMemoryOrDisk(
                "pool_swap_events", self.data_path, save=True
            )
            self.readFromMemoryOrDisk(
                "pool_mint_burn_events", self.data_path, save=True
            )

            max_bn_of_swaps = self.cache['swaps'].select('block_number').max().item()
            max_bn_of_mb = self.cache['mb'].select('block_number').max().item()

            self.max_supported = min(max_bn_of_mb, max_bn_of_swaps)

    def delete_tables(self, tables):
        """
        See pool_helpers.drop_tables
        """
        drop_tables(self, tables) 

    def readFromMemoryOrDisk(self, data, data_path, save=False):
        """
        Function that either returns a cached version for speed of
        the dataset or calculates them on the fly. This is used by all
        points that read data to 

        Notice: data is the table
        Notice: data_path is the saved path to the data
        Notice: pull=False caches the data instead of pulling
        """
        if data == "pool_swap_events":
            if "swaps" in self.cache.keys():
                return self.cache["swaps"]
            
            else:
                df = (
                        pl.scan_parquet(f"{data_path}/{data}/*.parquet")
                        .filter(
                            (pl.col("address") == self.pool)
                            & (pl.col("chain_name") == self.chain)
                        )
                        .with_columns(
                            as_of=pl.col("block_number") + pl.col("transaction_index") / 1e4
                        )
                        .collect()
                        .sort("as_of")
                    )
                if save:
                    self.cache["swaps"] = df

                return df 

        elif data == "pool_mint_burn_events":
            if "mb" in self.cache.keys():
                return self.cache["mb"]
            
            else:
                df = (
                    pl.scan_parquet(f"{data_path}/{data}/*.parquet")
                    .filter(
                        (pl.col("address") == self.pool)
                        & (pl.col("chain_name") == self.chain)
                    )
                    .cast({"amount": pl.Float64,
                            "tick_lower": pl.Int64,
                            "tick_upper": pl.Int64,
                            "type_of_event": pl.Float64})
                    .with_columns(
                        as_of=pl.col("block_number") + pl.col("transaction_index") / 1e4
                    )
                    .collect()
                    .sort("as_of")
                )
                if save:
                    self.cache["mb"] = df

                return df
        
    def calcSwapDF(self, as_of):
        """
        @inherit from pool_helpers.createSwapDF
        Helper function that calculates and caches swapDFs
        swapDFs are pre-computed datasets required for swap computation
        They are cached to optimize for multiple swaps at one as_of

        Notice: as_of is the block + transaction index / 1e4. 
        Notice: Returns the value before the transaction at that index was done
        """
        if self.cache['as_of'] == as_of:
            return self.cache["swapDF"], self.cache["inRangeValues"]
        
        as_of, df, inRangeValues = createSwapDF(as_of, self)

        self.cache["as_of"] = as_of
        self.cache["swapDF"] = df
        self.cache["inRangeValues"] = inRangeValues

        return df, inRangeValues

    def getPropertyFrom(self, as_of, pool_property):
        """
        Helper function that returns values from columns at the desired time

        Notice: as_of is the block + transaction index / 1e4. 
        Notice: Returns the value before the transaction at that index was done
        """
        pool_property = (
            self.readFromMemoryOrDisk("pool_swap_events", self.data_path)
            .filter(pl.col("as_of") < as_of)
            .tail(1)
            .select(pool_property)
            .item()
        )

        return pool_property

    def getTickAt(self, as_of, revert_on_uninitialized=False):
        """
        Returns the tick from the pool as_of the given block/transaction_index.

        Notice: as_of is the block + transaction index / 1e4. 
        Notice: Returns the value before the transaction at that index was done
        """
        tick = self.getPropertyFrom(as_of, "tick")

        if tick == None:
            assert not revert_on_uninitialized, "Tick is not initialized"
            return None
        else:
            return int(tick)
 
    def getPriceAt(self, as_of, revert_on_uninitialized=False):
        """
        Returns the sqrtPriceX96 from the pool as_of the given block/transaction_index.

        Notice: as_of is the block + transaction index / 1e4. 
        Notice: Returns the value before the transaction at that index was done
        """
        price = self.getPropertyFrom(as_of, "sqrtPriceX96")

        if price == None:
            assert not revert_on_uninitialized, "Price is not initialized"
            return None
        else:
            return int(price)
        
    def getPriceSeries(self, as_of, frequency='6h', gas = False):
        """
        @inhert from pool_helpers.getPriceSeries
        Create a price series resampled to the desired frequency 
        starting at as_of

        Notice: as_of is the block + transaction index / 1e4. 
        """
        px = getPriceSeries(self, as_of, frequency, gas)

        return px
    
    def getBNAtDate(self, as_of):
        """
        @inhert from pool_helpers.dtToBN
        Returns the last block number at that datetime

        Notice: as_of is the block + transaction index / 1e4. 
        """
        
        return dtToBN(as_of, self)
    
    def createLiq(self, as_of):
        """
        @inhert from pool_helpers.createSwapDF
        Creates a liquidity distribution as_of that time
        
        Notice: as_of is the block + transaction index / 1e4. 
        """
        
        return createLiq(as_of, self, "pool_mint_burn_events", self.data_path)
    
    def swapIn(self, calldata):
        """
        @inherit from swap.swapIn
        Simulates a swap using the given "calldata"

        Calldata takes the form:
        calldata = {# the time of the swap
                    'as_of': as_of, 
                    # the token address going in
                    'tokenIn': address 
                    # the amount of tokens going in
                    'swapIn': amount 
                    # skips the swap and calculates max amount out
                    'findMax': False,
                    # calculates fees accured to each tick 
                    'fees': True
                    } 
        
        Notice: as_of is the block + transaction index / 1e4. 
        """
        
        return swapIn(calldata, self)
    
    @property
    def swaps(self):
        """
        Getter for swaps
        """
        if not self.pull:
            return self.readFromMemoryOrDisk(
                    "pool_swap_events", self.data_path, pull=True
                )
        else:
            return self.cache['swaps']
    
    @property
    def mb(self):
        """
        Getter for mints/burns
        """
        if not self.pull:
            return self.readFromMemoryOrDisk(
                    "pool_mint_burn_events", self.data_path, pull=True
                )
        else:
            return self.cache['mb']

    @property
    def Q96(self):
        """
        Getter for Q96 which is commonly used in fixed point
        """
        return self._Q96
