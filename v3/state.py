from .helpers import *
import polars as pl
from google.cloud import bigquery
from pathlib import Path
PACKAGEDIR = Path(__file__).parent.absolute()

from datetime import date, timedelta, datetime, timezone

class v3Pool:
    def __init__(
        self,
        pool,
        chain,
        low_memory=False,
        update=False,
        initialize=True,
        tgt_max_rows=1_000_000,
    ):
        self._Q96 = 2**96
        self.tgt_max_rows = tgt_max_rows

        self.client = bigquery.Client()
        self.chain = chain
        self.pool = pool
        self.initialized = initialize
        self.low_memory = low_memory

        # lol
        self.cache = {}
        self.cache["as_of"] = 0

        # immutables
        self.tableToDB = {
            "uniswap-labs.on_chain_events.uniswap_v3_factory_pool_created_events_combined": "factory_pool_created",
            "uniswap-labs.on_chain_events.uniswap_v3_pool_swap_events_combined": "pool_swap_events",
            "uniswap-labs.on_chain_events.uniswap_v3_pool_mint_burn_events_combined": "pool_mint_burn_events",
            "uniswap-labs.on_chain_events.uniswap_v3_pool_initialize_events_combined": "pool_initialize_events",
        }

        self.proj_id = "uniswap-labs"
        self.db = "on_chain_events"

        # data checkers
        self.path = f"{PACKAGEDIR}/data"
        self.data_path = f"{PACKAGEDIR}/data"
        checkPath("", self.data_path)

        self.tables = [
            "uniswap_v3_factory_pool_created_events_combined",
            "uniswap_v3_pool_swap_events_combined",
            "uniswap_v3_pool_mint_burn_events_combined",
            "uniswap_v3_pool_initialize_events_combined",
        ]
        if update:
            self.update_tables()

            if self.chain == "optimism":
                print("Chain = optimism - Start pulling the ovm1")
                # the ovm1 = optimism, but they are in seperate databases
                # pain
                try:
                    self.chain = "optimism_legacy_ovm1"
                    self.update_tables()
                except Exception as e:
                    raise (e)
                # we dont want these race condition
                finally:
                    self.chain = "optimism"

        if initialize:
            self.ts, self.fee, self.token0, self.token1 = initializePoolFromFactory(
                pool, self.chain, self.data_path
            )
            self.swaps = self.readFromMemoryOrDisk(
                "pool_swap_events", self.data_path, pull=False
            )

    def update_tables(self, tables=[]):
        if tables == []:
            tables = self.tables

        for table in tables:
            print(f"Starting table {table}")
            gbq_table = f"{self.proj_id}.{self.db}.{table}"

            data_type = self.tableToDB[gbq_table]
            checkPath(data_type, self.data_path)

            # max row in gbq and min row in gbq
            max_block, min_block_of_segment = checkMinMaxBlock(
                gbq_table, self.client, self.chain
            )
            print(f"Found {min_block_of_segment} to {max_block}")

            # check if we already have data
            header = getHeader(data_type, self.data_path)
            # we already have existing data, so lets get the bn to only append new stuff
            if header != 0:
                print(f"Found data")
                found_min_block_of_segment = (
                    pl.scan_parquet(f"{self.path}/{data_type}/*.parquet")
                    .filter(pl.col("chain_name") == self.chain)
                    .select("block_number")
                    .max()
                    .collect()
                    .item()
                )
                # we may have data but it is for a diff chain
                if found_min_block_of_segment == None:
                    pass
                else:
                    min_block_of_segment = found_min_block_of_segment + 1
                print(f"Updated to {min_block_of_segment} to {max_block}")

            iterations = 0
            while max_block > min_block_of_segment:
                iterations += 1

                print(f"Starting at {min_block_of_segment}")
                # the finds the max block of the segment
                # which is the max block that returns close to the target amount of rows to pull from gbq
                max_block_of_segment = findSegment(
                    gbq_table,
                    min_block_of_segment,
                    self.client,
                    self.chain,
                    self.tgt_max_rows,
                )

                print(f"Going from {min_block_of_segment} to {max_block_of_segment}")
                # read that segment in from gbq
                df = readGBQ(
                    gbq_table,
                    max_block_of_segment,
                    min_block_of_segment,
                    self.client,
                    self.chain,
                )

                # save it down
                writeDataset(
                    df,
                    data_type,
                    self.data_path,
                    max_block_of_segment,
                    min_block_of_segment,
                )

                # we need the ovm1 state for the current optimism
                # so we read that state in and then we dump it as it happened
                # in the genesis block
                if self.chain == "optimism_legacy_ovm1":
                    # back fill and block_timestamp, block_number, and chain
                    mapping = readOVM("data", "mappings")

                    df = (
                        df.with_columns(
                            block_number=1,
                            # https://optimistic.etherscan.io/block/1
                            block_timestamp=datetime(
                                year=2021,
                                month=11,
                                day=11,
                                hour=21,
                                minute=16,
                                second=39,
                                tzinfo=timezone.utc,
                            ),
                            chain_name=pl.lit("optimism"),
                        )
                        # it defaults to int32 and we want 64
                        .cast({"block_number": pl.Int64})
                    )

                    if data_type in [
                        "pool_swap_events",
                        "pool_mint_burn_events",
                        "pool_initialize_events",
                    ]:
                        df = df.with_columns(
                            # ovm changed contract addresses from ovm1 to ovm2
                            # we map this back for us
                            address=pl.col("address").map_dict(mapping, default=None)
                        )

                    elif data_type in ["factory_pool_created"]:
                        df = df.with_columns(
                            # ovm changed contract addresses from ovm1 to ovm2
                            # we map this back for us
                            pool=pl.col("pool").map_dict(mapping, default=None)
                        )

                    # we index the optimism chain by backloading all the ovm1 data as optimism at block 0
                    writeDataset(df, data_type, self.data_path, 0, 0)

                # this moves the iteration, we pulled all of block n, so we want to start at n+1
                min_block_of_segment = max_block_of_segment + 1

            if iterations == 0:
                print("Nothing to update")

    def readFromMemoryOrDisk(self, data, data_path, pull=False):
        if data == "pool_swap_events":
            if self.low_memory or pull:
                return (
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
            else:
                if "swaps" not in self.cache.keys():
                    # re-use the code here to pull it
                    self.cache["swaps"] = self.readFromMemoryOrDisk(
                        data, data_path, True
                    )

                return self.cache["swaps"]

        elif data == "pool_mint_burn_events":
            if self.low_memory or pull:
                return (
                    pl.scan_parquet(f"{data_path}/{data}/*.parquet")
                    .filter(
                        (pl.col("address") == self.pool)
                        & (pl.col("chain_name") == self.chain)
                    )
                    .cast({"amount": pl.Float64,
                            "tick_lower": pl.Int64,
                            "tick_upper": pl.Int64,
                            "type_of_event": pl.Float64})
                    .collect()
                    .sort("block_number")
                )
            else:
                if "mb" not in self.cache.keys():
                    self.cache["mb"] = self.readFromMemoryOrDisk(data, data_path, True)

                return self.cache["mb"]

    def calcSwapDF(self, as_of):
        as_of, df, inRangeValues = createSwapDF(as_of, self)

        self.cache["as_of"] = as_of
        self.cache["swapDF"] = df
        self.cache["inRangeValues"] = inRangeValues

        return df, inRangeValues

    def getPropertyFrom(self, as_of, pool_property):
        pool_property = (
            self.readFromMemoryOrDisk("pool_swap_events", self.data_path)
            .filter(pl.col("as_of") < as_of)
            .tail(1)
            .select(pool_property)
            .item()
        )

        return pool_property

    def getTickAt(self, as_of, revert_on_uninitialized=False):
        tick = self.getPropertyFrom(as_of, "tick")

        if tick == None:
            assert not revert_on_uninitialized, "Tick is not initialized"
            return None
        else:
            return int(tick)

    def getPriceAt(self, as_of, revert_on_uninitialized=False):
        price = self.getPropertyFrom(as_of, "sqrtPriceX96")

        if price == None:
            assert not revert_on_uninitialized, "Price is not initialized"
            return None
        else:
            return int(price)
        
    def getPriceSeries(self, as_of):
        px = getPriceSeries(self, as_of)

        return px
    
    def getBNAtDate(self, as_of):
        return dtToBN(as_of, self)
    
    # bn, pool, data, data_path
    def createLiq(self, as_of):
        return createLiq(as_of, self, "pool_mint_burn_events", self.data_path)

    @property
    def getSwaps(self):
        assert self.initialized, "Pool not initialized"

        return self.swaps

    @property
    def Q96(self):
        return self._Q96
