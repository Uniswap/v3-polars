from .helpers import *
import polars as pl
from pathlib import Path

PACKAGEDIR = Path(__file__).parent.absolute()


class v3Pool:
    def __init__(
        self,
        pool,
        chain,
        update_from="gcp",
        low_memory=False,
        update=False,
        pull=True,
        tgt_max_rows=200_000,
        test_mode=False,
        tables = []
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

        # state where simulations are cached
        self.slot0 = {
            "initialized": False,
            "next_blocks": pl.DataFrame(),
            "prev_blocks": pl.DataFrame(),
            "next_block": pl.DataFrame(),
            "prev_block": pl.DataFrame(),
            "liquidity": pl.DataFrame(),
            "as_of": -1,
        }

        # this is the cache where we store data if needed
        # this is used to move disk -> memory -> optimized memory
        self.cache = {}

        # data checkers
        self.path = str(Path(f"{PACKAGEDIR}/data").resolve())
        self.data_path = str(Path(f"{PACKAGEDIR}/data").resolve())
        checkPath("", self.data_path)

        # tables to update
        if tables == []:
            self.tables = [
                "factory_pool_created",
                "pool_initialize_events",
                "pool_swap_events",
                "pool_mint_burn_events",
            ]
        else:
            assert type(tables) == list, "Please provide a list for tables"
            self.tables = tables

        self.connector = None
        # data quality assurances
        self.max_supported = -1

        if update:
            update_tables(self, update_from, self.tables, test_mode)

            if self.chain == "optimism":
                print("Chain = optimism - Start pulling the ovm1")
                # the ovm1 = optimism, but they are in seperate databases
                # pain
                try:
                    self.chain = "optimism_legacy_ovm1"
                    update_tables(self, update_from, self.tables, test_mode)
                except Exception as e:
                    raise (e)
                # we dont want these race condition
                finally:
                    self.chain = "optimism"
        else:
            if test_mode:
                raise ValueError("test_mode true but update false")

        self.ts, self.fee, self.token0, self.token1 = initializePoolFromFactory(
            self.pool, self.chain, self.data_path
        )
        if test_mode:
            test_assertion(self)

        if pull:
            self.readFromMemoryOrDisk("pool_swap_events", self.data_path, save=True)
            self.readFromMemoryOrDisk(
                "pool_mint_burn_events", self.data_path, save=True
            )

            max_bn_of_swaps = self.cache["swaps"].select("block_number").max().item()
            max_bn_of_mb = self.cache["mb"].select("block_number").max().item()

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
                        # TODO
                        # will overflow if block has more than 10k txs
                        # need to think of a better way lol
                        as_of=pl.col("block_number")
                        + pl.col("transaction_index") / 1e4
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
                    .cast(
                        {
                            "amount": pl.Float64,
                            "tick_lower": pl.Int64,
                            "tick_upper": pl.Int64,
                            "type_of_event": pl.Float64,
                        }
                    )
                    .with_columns(
                        as_of=pl.col("block_number") + pl.col("transaction_index") / 1e4
                    )
                    .collect()
                    .sort("as_of")
                )
                if save:
                    self.cache["mb"] = df

                return df

    def calcSwapDF(self, as_of, force = False):
        """
        @inherit from pool_helpers.createSwapDF
        Helper function that calculates and caches swapDFs
        swapDFs are pre-computed datasets required for swap computation
        They are cached to optimize for multiple swaps at one as_of

        Notice: as_of is the block + transaction index / 1e4.
        Notice: Returns the value before the transaction at that index was done
        Notice: we attempt to optimistically shift state if possible, but you can 
                instead force the code to recalculate
        """
        # this is initalized after first swap run
        rotationValid = False
        if self.slot0["initialized"]:
            next_block = slot0ToAsOf(self.slot0["next_block"])
            prev_block = slot0ToAsOf(self.slot0["prev_block"])

            # state is still valid and we can just rotate as_of
            # NOTE: we replace the tx at as_of with our tx
            # thus if txs would be equal to as_of, then we replace them
            # which is why we equal here
            if ((prev_block <= as_of) and 
                (as_of <= next_block) and 
                (not force)):

                self.slot0["as_of"] = as_of
                return self.cache["swapDF"], self.cache["inRangeValues"]
            
            # TODO instead of recalculating liquidty from scratch, we can calculate
            # based off a range and apply the deltas to the liquidity distributions
            else:
                if force:
                    # rotationValid = False
                    # this should already be False so noop
                    pass

                elif as_of > next_block:
                    entry = (
                        self.slot0["next_blocks"]
                        .filter(pl.col("type_of_int") != "swap")
                        .head(1)
                    )
                    # we may not have pulled the entry
                    if entry.is_empty():
                        # since there are no mbs, we know up until
                        # the last of prev blocks is valid
                        entry = self.slot0["prev_blocks"].tail(1)
                    nextMB = slot0ToAsOf(entry)
                    if nextMB > as_of:
                        rotationValid = True

                elif as_of < prev_block:
                    entry = (
                        self.slot0["prev_blocks"]
                        .filter(pl.col("type_of_int") != "swap")
                        .head(1)
                    )

                    if entry.is_empty():
                        entry = self.slot0["prev_blocks"].tail(1)

                    nextMB = slot0ToAsOf(entry)
                    if nextMB < as_of:
                        rotationValid = True
                
        as_of, df, inRangeValues = createSwapDF(as_of, self, rotationValid)

        self.slot0["as_of"] = as_of
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
        )

        if pool_property.is_empty():
            return None

        return pool_property

    def getTickAt(self, as_of, revert_on_uninitialized=False):
        """
        Returns the tick from the pool as_of the given block/transaction_index.

        Notice: as_of is the block + transaction index / 1e4.
        Notice: Returns the value before the transaction at that index was done
        """
        tick = self.getPropertyFrom(as_of, "tick")

        if type(tick) is not pl.DataFrame:
            assert not revert_on_uninitialized, "Tick is not initialized"
            return None
        else:
            return int(tick.item())

    def getPriceAt(self, as_of, revert_on_uninitialized=False):
        """
        Returns the sqrtPriceX96 from the pool as_of the given block/transaction_index.

        Notice: as_of is the block + transaction index / 1e4.
        Notice: Returns the value before the transaction at that index was done
        """
        price = self.getPropertyFrom(as_of, "sqrtPriceX96")

        if type(price) is not pl.DataFrame:
            assert not revert_on_uninitialized, "Price is not initialized"
            return None
        else:
            return int(price)

    def getPriceSeries(self, as_of, ending=None, frequency="6h", gas=False):
        """
        @inhert from pool_helpers.getPriceSeries
        Create a price series resampled to the desired frequency
        starting at as_of

        Notice: as_of is the block + transaction index / 1e4.
        """

        px = getPriceSeries(self, as_of, ending, frequency, gas)

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
            return self.cache["swaps"]

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
            return self.cache["mb"]

    @property
    def Q96(self):
        """
        Getter for Q96 which is commonly used in fixed point
        """
        return self._Q96
