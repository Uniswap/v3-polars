import polars as pl
import os


# data updating
def checkPath(data_type, data_path):
    path = f"{data_path}/{data_type}"

    if not os.path.exists(path):
        os.mkdir(path)


def isDS_Store(f):
    return f != ".DS_Store"


def getHeader(table, data_path):
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
    idx = getHeader(data_type, data_path)
    df.write_parquet(
        f"{data_path}/{data_type}/{idx}_{min_block_of_segment}_{max_block_of_segment}_{data_type}.parquet"
    )


def readGBQ(gbq_table, max_block_of_segment, min_block_of_segment, client, chain):
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
    q = f"""select max(block_number)
            from (
                select * 
                  from (
                    select block_number
                    FROM `{gbq_table}` 
                    where chain_name = '{chain}'
                    and block_number >= {min_block}
                    order by block_number asc
                  ) limit {tgt_max_rows}
            )
         """

    query_job = client.query(q)  # API request
    rows = query_job.result()  # Waits for query to finish

    df = pl.from_arrow(rows.to_arrow())

    return df.item()
