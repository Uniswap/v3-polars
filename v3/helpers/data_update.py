import polars as pl
import os
from datetime import date, timedelta, datetime, timezone

gcp_locked = True
try:
    from google.cloud import bigquery

    gcp_locked = False
except ImportError:
    print("Unable to import GCP")


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


def writeDataset(df, data_type, data_path, max_block_of_segment, min_block_of_segment):
    """
    Writes the given file with the given heuristics to the disk
    """
    if df.is_empty():
        print("Data pass to save is empty - passing")
    else:
        idx = getHeader(data_type, data_path)
        df.write_parquet(
            f"{data_path}/{data_type}/{idx}_{min_block_of_segment}_{max_block_of_segment}_{data_type}.parquet"
        )


def readGBQ(gbq_table, max_block_of_segment, min_block_of_segment, client, chain):
    """
    Pull from internal GBQ data lake
    """
    q = f"""select * 
        FROM `{gbq_table}` 
        WHERE chain_name = '{chain}'
        AND block_number <= {max_block_of_segment}
        AND block_number >= {min_block_of_segment}
        """

    query_job = client.query(q)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())

    return df


def checkMinMaxBlock(gbq_table, client, chain):
    """
    Find the max and min row in the database to pull over
    """
    # read the first entry
    q = f"""select min(block_number) as min_block,
                   max(block_number) as max_block,
                   FROM `{gbq_table}` 
                   where chain_name = '{chain}'
         """
    query_job = client.query(q)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())
    return df["max_block"].item(), df["min_block"].item()


def findSegment(gbq_table, min_block, client, chain, tgt_max_rows):
    """
    We want to find the smallest block such that we are pulling
    around the tgt_max_rows number of rows from GBQ
    """
    q = f"""select max(block_number)
            from (
                select * 
                  from (
                    select block_number,
                            row_number() over (order by block_timestamp asc) as rownum
                    FROM `{gbq_table}` 
                    where chain_name = '{chain}'
                    and block_number >= {min_block}
                    order by block_timestamp asc
                  ) 
                where rownum <= {tgt_max_rows}
            )
         """

    query_job = client.query(q)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())

    return df.item()


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


def update_tables_gbq(pool, tables=[]):
    """
    This is the big file that pulls from the GBQ servers for the given
    pool and requested tables
    """
    # bigquery strings -> python
    proj_id = "uniswap-labs"
    db = "on_chain_events"
    client = bigquery.Client(project=proj_id)

    tableToDB = {
        "uniswap-labs.on_chain_events.uniswap_v3_factory_pool_created_events_combined": "factory_pool_created",
        "uniswap-labs.on_chain_events.uniswap_v3_pool_swap_events_combined": "pool_swap_events",
        "uniswap-labs.on_chain_events.uniswap_v3_pool_mint_burn_events_combined": "pool_mint_burn_events",
        "uniswap-labs.on_chain_events.uniswap_v3_pool_initialize_events_combined": "pool_initialize_events",
    }

    if tables == []:
        tables = pool.tables

    for table in tables:
        print(f"Starting table {table}")
        gbq_table = f"{proj_id}.{db}.{table}"

        data_type = tableToDB[gbq_table]
        checkPath(data_type, pool.data_path)

        # max row in gbq and min row in gbq
        max_block, min_block_of_segment = checkMinMaxBlock(
            gbq_table, client, pool.chain
        )
        print(f"Found {min_block_of_segment} to {max_block}")

        # check if we already have data
        header = getHeader(data_type, pool.data_path)
        # we already have existing data, so lets get the bn to only append new stuff
        if header != 0:
            print(f"Found data saved")
            found_min_block_of_segment = (
                pl.scan_parquet(f"{pool.path}/{data_type}/*.parquet")
                .filter(pl.col("chain_name") == pool.chain)
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
                client,
                pool.chain,
                pool.tgt_max_rows,
            )

            print(f"Going from {min_block_of_segment} to {max_block_of_segment}")
            # read that segment in from gbq
            df = readGBQ(
                gbq_table,
                max_block_of_segment,
                min_block_of_segment,
                client,
                pool.chain,
            )

            # save it down
            writeDataset(
                df,
                data_type,
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
                writeDataset(df, data_type, pool.data_path, 0, 0)

            # this moves the iteration, we pulled all of block n, so we want to start at n+1
            min_block_of_segment = max_block_of_segment + 1

        if iterations == 0:
            print("Nothing to update")


def update_tables_cryo(pool, tables=[]):
    """
    sad
    """
    # TODO
    raise NotImplementedError("Cryo is not yet implimented")


def update_tables(pool, update_from, tables=[]):
    if update_from == "gcp":
        assert not gcp_locked, "GCP could not be imported"
        update_tables_gbq(pool, tables)
    elif update_from == "cryo":
        update_tables_cryo(pool, tables)
    else:
        raise NotImplementedError("Data puller not implimented")
