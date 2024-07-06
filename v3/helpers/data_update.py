import polars as pl
import os
from datetime import date, timedelta, datetime, timezone
from .connectors import allium, gbq
from .test_helpers import *
from pathlib import Path
import json


# data updating
def checkPath(data_type, data_path):
    """
    Checks if the path exists and creates it is not
    """
    path = f"{data_path}/{data_type}"

    if not os.path.exists(path):
        os.mkdir(path)


def isDS_Store(f):
    """
    All my homies hate the .DS_Store, so we check
    if the file is .DS_Store before iterating over files
    """
    return f != ".DS_Store"


def getHeader(table, data_path):
    """
    Returns an increasing number that will ensure that there
    are no collisions. Pulls that number from the files and
    then adds 1 to it.
    """
    path = f"{data_path}/{table}"

    # this makes the assumption that the files are in the correct directory
    files = os.listdir(path)

    # split the first index and select the max
    max_index = [int(f.split("_")[0]) for f in files if isDS_Store(f)]

    # if there is no files or there is nothing in the max_index
    # then we return 0
    if len(files) == 0 or len(max_index) == 0:
        return 0

    return max(max_index) + 1


def writeDataset(df, table, data_path, max_block_of_segment, min_block_of_segment):
    """
    Writes the given file with the given heuristics to the disk
    """
    idx = getHeader(table, data_path)
    if not df.is_empty():
        df.write_parquet(
            f"{data_path}/{table}/{idx}_{min_block_of_segment}_{max_block_of_segment}_{table}.parquet",
        )


def readRemote(
    table, connector, max_block_of_segment, min_block_of_segment, pool, chain
):
    """
    Read the raw dataframe from remote
    """

    q = connector.get_template(
        "read", table, max_block_of_segment, min_block_of_segment, pool, chain
    )
    df = connector.execute(q)

    return df


def checkGlobalMinMaxBlock(table, connector, pool, chain):
    """
    Find the max and min row in the database to pull over
    """

    # read the first entry
    q = connector.get_template("minMax", table, pool, chain)
    df = connector.execute(q)

    print(df)
    if not df.is_empty():
        return df["max_block"].item(), df["min_block"].item()
    else:
        return -1, -1


def findSegment(table, connector, max_block, min_block, pool, chain, tgt_max_rows):
    """
    We want to find the smallest block such that we are pulling
    around the tgt_max_rows number of rows from GBQ
    """

    q = connector.get_template(
        "findSegment", table, max_block, min_block, pool, chain, tgt_max_rows
    )
    df = connector.execute(q)

    return df.item() - 1


def readOVM(path, data_type):
    """
    This is provided by the Optimism team to
    map OVM1 -> the current EVM addresses
    """
    mappings = {
        old: new
        for old, new in (
            pl.read_csv(f"{path}/{data_type}/ovm_mapping.csv")
            .select(["oldaddress", "newaddress"])
            .iter_rows()
        )
    }

    return mappings


def _update_tables(pool, tables=[], test_mode=False):
    if test_mode:
        check_test_mode(pool)
        pool.data_path = f"{pool.data_path}/test"
        checkPath("", pool.data_path)

        for p in Path(pool.data_path).iterdir():
            if not p.is_dir():
                continue
            for x in p.iterdir():
                x.unlink(missing_ok=True)

        # 1000th swap on mainnet happened at this block
        max_block = 12376625

    if tables == [] or test_mode:
        tables = pool.tables

    for table in tables:
        print(f"Starting table {table}")
        checkPath(table, pool.data_path)

        # max row in gbq and min row in remote
        max_block, min_block_of_segment = checkGlobalMinMaxBlock(
            table, pool.connector, pool.pool, pool.chain
        )

        if max_block == -1:
            print(f"Failed to find table for {table}")
            continue

        print(f"Found {min_block_of_segment} to {max_block}")

        if test_mode:
            check_min_segment(min_block_of_segment, table)
            print(f"Check remote max for table {table} is {max_block}")
            max_block = 12376625

        # check if we already have data
        header = getHeader(table, pool.data_path)
        # we already have existing data, so lets get the bn to only append new stuff
        if header != 0:

            # we pull all data on factory_pool_created - so we override the filter to be true
            optimistic_address_filter = True
            if table in ["pool_swap_events", "pool_mint_burn_events"]:
                optimistic_address_filter = pl.col("address") == pool.pool

            found_min_block_of_segment = (
                pl.scan_parquet(f"{pool.data_path}/{table}/*.parquet")
                .filter(
                    (pl.col("chain_name") == pool.chain) & (optimistic_address_filter)
                )
                .select("block_number")
                .max()
                .collect()
                .item()
            )
            # we may have data but it is for a diff chain
            if found_min_block_of_segment == None:
                pass
            else:
                assert not test_mode, "Found loaded test folder"
                min_block_of_segment = found_min_block_of_segment + 1

            print(f"Found data - Updated to {min_block_of_segment} to {max_block}")

        iterations = 0
        while max_block > min_block_of_segment:
            iterations += 1

            print(f"Starting at {min_block_of_segment}")
            # the finds the max block of the segment
            # which is the max block that returns close to the target amount of rows to pull from gbq
            max_block_of_segment = findSegment(
                table,
                pool.connector,
                max_block,
                min_block_of_segment,
                pool.pool,
                pool.chain,
                pool.tgt_max_rows,
            )

            print(f"Going from {min_block_of_segment} to {max_block_of_segment}")
            # read that segment in from remote
            df = readRemote(
                table,
                pool.connector,
                max_block_of_segment,
                min_block_of_segment,
                pool.pool,
                pool.chain,
            )

            # if there was no data, `df` will be empty
            if df.is_empty():
                print(
                    f"No data found for {min_block_of_segment} to {max_block_of_segment}"
                )
                break

            # save it down
            writeDataset(
                df,
                table,
                pool.data_path,
                max_block_of_segment,
                min_block_of_segment,
            )

            # we need the ovm1 state for the current optimism
            # so we read that state in and then we dump it as it happened
            # in the genesis block
            if pool.chain == "optimism_legacy_ovm1":
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

                if table in [
                    "pool_swap_events",
                    "pool_mint_burn_events",
                    "pool_initialize_events",
                ]:
                    df = df.with_columns(
                        # ovm changed contract addresses from ovm1 to ovm2
                        # we map this back for us
                        address=pl.col("address").map_dict(mapping, default=None)
                    )

                elif table in ["factory_pool_created"]:
                    df = df.with_columns(
                        # ovm changed contract addresses from ovm1 to ovm2
                        # we map this back for us
                        pool=pl.col("pool").map_dict(mapping, default=None)
                    )

                # we index the optimism chain by backloading all the ovm1 data as optimism at block 0
                writeDataset(df, table, pool.data_path, 0, 0)

            # this moves the iteration, we pulled all of block n, so we want to start at n+1
            if not df.is_empty():
                min_block_of_segment = df.select("block_number").max().item() + 1
            else:
                # if we didn't pull anything, it may still be fine
                # this is likely because no data exists in the pulled sample
                # the failure moat should be captured by the connectors
                min_block_of_segment = max_block_of_segment + 1

            if test_mode:
                break

        if iterations == 0:
            print("Nothing to update")


def update_tables(pool, update_from, tables=[], test_mode=False):
    if update_from == "gcp":
        gcp_locked = True
        try:
            from google.cloud import bigquery

            gcp_locked = False
        except ImportError:
            raise Exception(
                "GCP could not be imported. If you want to use another source (such as allium), set update_from to the desired source e.g. 'allium'"
            )

        pool.connector = gbq()
        _update_tables(pool, tables, test_mode)

    elif update_from == "allium":
        assert (
            pool.tgt_max_rows <= 200_000
        ), "Attempting to pull too many rows (>200k), set tgt_max_rows to less than 100k rows"

        allium_query_id = os.getenv("ALLIUM_POLARSV3_QUERY_ID")
        allium_api_key = os.getenv("ALLIUM_POLARSV3_API_KEY")

        assert (
            allium_query_id and allium_api_key
        ), "Please set ALLIUM_POLARSV3_QUERY_ID and ALLIUM_POLARSV3_API_KEY environment variables"

        pool.connector = allium(allium_query_id, allium_api_key)
        _update_tables(pool, tables, test_mode)

    elif update_from == "cryo":
        raise NotImplementedError("sad")
        # _update_tables(pool, tables, test_mode)
    else:
        raise NotImplementedError("Data puller not implemented")
