import polars as pl
import os
from datetime import date, timedelta, datetime, timezone
from polars.testing import assert_frame_equal
from pathlib import Path


def check_min_segment(value, table):
    """
    check hash 0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf
    for swaps: 0x2bdb4298b35adf058a38dfbe85470f67da1cb76e169496f9fa04fd19fb153274
    """
    tgt = 12369739
    if table == "pool_swap_events":
        tgt = 12369879

    err = f"Incorrect lower bound for {table} - expected {tgt} but got {value}"
    assert tgt == value, err


def check_test_mode(pool):
    err = "Test mode requires chain_id to be ethereum"
    assert pool.chain == "ethereum", err


def test_assertion(pool):
    for table in [
        "pool_swap_events",
        "pool_mint_burn_events",
        "pool_initialize_events",
        "factory_pool_created",
    ]:
        ordering = ["block_number", "transaction_index", "log_index"]
        if table == "factory_pool_created":
            ordering = ["block_number", "log_index"]

        test = (
            pl.scan_parquet(f"{pool.data_path}/{table}/*.parquet")
            .sort(ordering)
            .collect()
        )

        test = test.select(sorted(test.columns))

        # walk it back one folder
        p = str(Path(pool.data_path).parent)

        examples = (
            pl.scan_parquet(f"{p}/examples/{table}/*.parquet").sort(ordering).collect()
        )

        examples = examples.select(sorted(examples.columns))

        try:
            assert_frame_equal(test, examples)
        except AssertionError as e:
            print(f"Failed iteration on {table}")
            raise e

    print("All tests passed")
