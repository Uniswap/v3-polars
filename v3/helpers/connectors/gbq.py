import polars as pl
import os
from datetime import date, timedelta, datetime, timezone


def get_gbq_client(proj_id):
    try:
        from google.cloud import bigquery

        return bigquery.Client(project=proj_id)
    except ImportError:
        raise Exception(
            "GCP could not be imported. If you want to use another source (such as allium), set update_from to the desired source e.g. 'allium'"
        )


class gbq:
    def __init__(self):
        self.proj_id = "uniswap-labs"
        self.db = "on_chain_events"

        self.client = get_gbq_client(self.proj_id)

        self.remote_tables = {
            "factory_pool_created": "uniswap_v3_factory_pool_created_events_combined",
            "pool_swap_events": "uniswap_v3_pool_swap_events_combined",
            "pool_mint_burn_events": "uniswap_v3_pool_mint_burn_events_combined",
            "pool_initialize_events": "uniswap_v3_pool_initialize_events_combined",
        }

    def get_remote_table(self, table):
        return f"{self.proj_id}.{self.db}.{self.remote_tables[table]}"

    def minMax(self, *args):
        """
        We want to find the bounds of the remote database
        """
        table, chain = args
        table = self.get_remote_table(table)

        q = f"""select min(block_number) as min_block,
                   max(block_number) as max_block,
                   FROM `{table}`
                   where chain_name = '{chain}'
             """

        return q

    def findSegment(self, *args):
        """
        We want to find the smallest block such that we are pulling
        around the tgt_max_rows number of rows from GBQ
        """
        table, max_block, min_block, chain, tgt_max_rows = args
        table = self.get_remote_table(table)

        q = f"""select max(block_number)
                from (
                    select * 
                    from (
                        select block_number
                        FROM `{table}`
                        where chain_name = '{chain}'
                        and block_number >= {min_block}
                        and block_number <= {max_block}
                        order by block_timestamp asc
                    ) limit {tgt_max_rows}
                )
            """

        return q

    def readRemote(self, *args):
        """
        Pull from internal GBQ data lake
        """
        table, max_block_of_segment, min_block_of_segment, chain = args
        table = self.get_remote_table(table)

        q = f"""select * 
            FROM `{table}`
            WHERE chain_name = '{chain}'
            AND block_number <= {max_block_of_segment}
            AND block_number >= {min_block_of_segment}
            """

        return q

    def get_template(self, query_type, *args):
        if query_type == "minMax":
            return self.minMax(*args)
        elif query_type == "findSegment":
            return self.findSegment(*args)
        elif query_type == "read":
            return self.readRemote(*args)
        else:
            raise ValueError("Missing table definition")

    def execute(self, q):
        query_job = self.client.query(q)  # API request
        rows = query_job.result()  # Waits for query to finish

        df = pl.from_arrow(rows.to_arrow())

        return df
