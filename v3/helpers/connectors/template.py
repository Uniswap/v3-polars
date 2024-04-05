import polars as pl
import os
from datetime import date, timedelta, datetime, timezone


class connector_template:
    '''
    this connector returns the query then executes the query
    
    the return type of the execute needs to be a polars dataframe
    '''

    def __init__(self):
        pass

    def get_remote_table(self, table) -> str:
        """
        map the local name for the event to the desired global table

        "factory_pool_created" -> remote database
        "pool_swap_events",
        "pool_mint_burn_events",
        "pool_initialize_events",
        """
        pass

    def minMax(self, *args) -> str:
        """
        We want to find the bounds of the remote database

        This is the min and max to iterate over for updating.
        min block is overridden to the local min block later

        needs to get out:
        two columns (max_block, min_block) that return ints
        of the max and min block in the remote sample

        NOTE: we cant unfurl this right now in the query because
        of how the generizied query executer works

        # in date_update.py
        return df["max_block"].item(), df["min_block"].item()
        """
        table, chain = args
        table = self.get_remote_table(table)

        q = f"""select min(block_number) as min_block,
                   max(block_number) as max_block,
                   FROM `{table}`
                   where chain_name = '{chain}'
             """

        return q

    def findSegment(self, *args) -> str:
        """
        We want to only pull a customizable amount of rows to ensure there are
        no issues with memory/time outs.

        Probably can be optimized a bunch

        needs to get out:
        one column (any column name works because item unfurls it) 
        return the block where there exists 1m rows below it

        NOTE: we cant unfurl this right now in the query because
        of how the generizied query executer works

        # in date_update.py
        return df.item()
        """
        table, min_block, chain, tgt_max_rows = args
        table = self.get_remote_table(table)

        q = f"""select max(block_number)
                from (
                    select * 
                    from (
                        select block_number
                        FROM `{table}`
                        where chain_name = '{chain}'
                        and block_number >= {min_block}
                        order by block_timestamp asc
                    ) limit {tgt_max_rows}
                )
            """

        return q

    def readRemote(self, *args) -> str:
        """
        Pull from internal GBQ data lake

        returns a dataframe of polars with the desired row schema
        the schemas can be found in data/examples/{table}

        NOTE: generally these look like 
        events with block info + tx index + gas costs

        specific schemas can be found in those examples 

        # in date_update.py
        return df
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

    def get_template(self, query_type, *args) -> str:
        """
        take the query type and pass the args to the right func

        this is then passed back to function and then execute() is
        called on it.

        TODO:
        check if i should just unite execute and get_template into one function
        """
        if query_type == "minMax":
            return self.minMax(*args)
        elif query_type == "findSegment":
            return self.findSegment(*args)
        elif query_type == "read":
            return self.readRemote(*args)
        else:
            raise ValueError("Missing table definition")

    def execute(self, q) -> pl.DataFrame:
        """
        Take the query and actually execute it
        and get the polars df return
        """
        query_job = self.client.query(q)  # API request
        rows = query_job.result()  # Waits for query to finish

        df = pl.from_arrow(rows.to_arrow())

        return df
