# data helpers
from .swap_math import *
import polars as pl
import os
import time

from datetime import date, timedelta, datetime, timezone


def initializePoolFromFactory(addr, chain, data_path):
    """
    Looks at the factory and pulls the needed data
    about the current pool initialization

    This is only available from the factory
    """
    data_type = "factory_pool_created"

    factory = (
        pl.scan_parquet(f"{data_path}/{data_type}/*.parquet")
        .filter((pl.col("pool") == addr) & (pl.col("chain_name") == chain))
        .collect()
    )

    assert factory.shape[0] != 0, "Pool missing from factory"
    assert not factory.shape[0] > 1, "Multiple pools at that address"

    ts = int(factory["tickSpacing"].item())
    fee = int(float(factory["fee"].item()))

    token0 = factory["token0"].item()
    token1 = factory["token1"].item()

    return ts, fee, token0, token1


def ceil_dt(dt, delta):
    """
    Helper for ceiling the datettime
    """
    return dt + (datetime.min - dt) % delta


def dtToBN(dt, pool):
    """
    For that chain, pulls the data (out of all swap) and gets the closet datetime
    """
    bn_as_of = (
        pl.scan_parquet(f"{pool.data_path}/pool_swap_events/*.parquet")
        .filter(
            (pl.col("chain_name") == pool.chain)
            & (pl.col("block_timestamp") >= dt.replace(tzinfo=timezone.utc))
        )
        .select(pl.col("block_number"))
        .max()
        .collect()
        .item()
    )

    return bn_as_of


def createSwapDF(as_of, pool, provided = 0):
    """
    This creates the swap data from that pre-computes most of the values
    needed to simulate a swap

    it gets the current pool price, and then created the liquidity distribution
    at that block, then calculates the amount available to trade.

    it then pre-computes the amounts needed to escape out of the current
    range as well
    """
    if provided == 0:
        price = pool.getPriceAt(as_of)
        assert price != None, "Pool not initialized"
    else:
        price = provided

    tickFloor = priceX96ToTickFloor(price, pool.ts)
    liq = createLiq(as_of, pool, "pool_mint_burn_events", pool.data_path)

    swap_df = (
        liq.filter(pl.col("liquidity") > 0)  # numerical error
        .with_columns(tick_b=pl.col("tick").shift(-1), tick_a=pl.col("tick"))
        .select(["liquidity", "tick_a", "tick_b"])
        .fill_null((pool.MAX_TICK // pool.ts) * pool.ts)
        .with_columns(
            p_a=(1.0001 ** pl.col("tick_a")) ** (1 / 2),
            p_b=(1.0001 ** pl.col("tick_b")) ** (1 / 2),
        )
        .with_columns(
            yInTick=pl.col("liquidity") * (pl.col("p_b") - pl.col("p_a")),
            xInTick=pl.col("liquidity")
            * ((pl.col("p_b") - pl.col("p_a")) / (pl.col("p_b") * pl.col("p_a"))),
        )
    )

    current_tick = swap_df.filter(
        (pl.col("tick_a") <= tickFloor) & ((pl.col("tick_b") > tickFloor))
    )

    if current_tick.shape[0] != 1:
        raise ValueError(
            f"Missing/Duplicate in-range tick - Size of {current_tick.shape[0]}"
        )

    sqrt_P = price / 2**96
    p_a, p_b, liquidity, tick = (
        current_tick["p_a"].item(),
        current_tick["p_b"].item(),
        current_tick["liquidity"].item(),
        current_tick["tick_a"].item(),
    )

    inRange0 = get_amount0_delta(p_a, sqrt_P, liquidity)
    inRangeToSwap0 = get_amount1_delta(p_a, sqrt_P, liquidity)

    inRange1 = get_amount1_delta(p_b, sqrt_P, liquidity)
    inRangeToSwap1 = get_amount0_delta(p_b, sqrt_P, liquidity)

    return (
        as_of,
        swap_df,
        (
            sqrt_P,
            inRange0,
            inRangeToSwap0,
            inRange1,
            inRangeToSwap1,
            liquidity,
            tick,
        ),
    )


def getPriceSeries(pool, start_time, frequency, gas=False):
    # precompute a dataframe that has the latest block number
    bn_as_of = (
        pl.scan_parquet(f"{pool.data_path}/pool_swap_events/*.parquet")
        .filter(
            (pl.col("chain_name") == pool.chain)
            & (pl.col("block_timestamp") >= start_time.replace(tzinfo=timezone.utc))
        )
        .select(["block_timestamp", "block_number"])
        .unique()
        .sort("block_timestamp")
        .group_by("block_timestamp")
        .last()
        .sort("block_timestamp")
        .group_by_dynamic("block_timestamp", every=frequency)
        .agg(pl.col("block_number").max())
        .collect()
    )

    if gas:
        tick_as_of = (
            pl.scan_parquet(f"{pool.data_path}/pool_swap_events/*.parquet")
            .filter(
                (pl.col("chain_name") == pool.chain)
                & (pl.col("address") == pool.pool)
                & (pl.col("block_timestamp") >= start_time.replace(tzinfo=timezone.utc))
            )
            .select(["block_timestamp", "tick", "gas_price", "gas_used"])
            .unique()
            .sort("block_timestamp")
            .group_by("block_timestamp")
            .last()
            .sort("block_timestamp")
            .cast({"tick": pl.Int64, "gas_price": pl.UInt64, "gas_used": pl.UInt64})
            .group_by_dynamic("block_timestamp", every=frequency)
            .agg(
                [
                    pl.col("tick").last().alias("tick"),
                    pl.col("gas_price").quantile(0.5).alias("gas_price"),
                    pl.col("gas_used").quantile(0.5).alias("gas_used"),
                ]
            )
            .with_columns(
                gas_price=pl.col("gas_price").forward_fill(),
                gas_used=pl.col("gas_used").forward_fill(),
                tick=pl.col("tick").forward_fill(),
            )
            .collect()
        )
    else:
        tick_as_of = (
            pl.scan_parquet(f"{pool.data_path}/pool_swap_events/*.parquet")
            .filter(
                (pl.col("chain_name") == pool.chain)
                & (pl.col("address") == pool.pool)
                & (pl.col("block_timestamp") >= start_time.replace(tzinfo=timezone.utc))
            )
            .select(["block_timestamp", "tick"])
            .unique()
            .sort("block_timestamp")
            .group_by("block_timestamp")
            .last()
            .sort("block_timestamp")
            .cast({"tick": pl.Int64})
            .group_by_dynamic("block_timestamp", every=frequency)
            .agg([pl.col("tick").last().alias("tick")])
            .collect()
        )
    price = bn_as_of.join_asof(tick_as_of, on="block_timestamp")

    return price


def drop_tables(pool, tables):
    # support both strings and lists
    if type(tables) != list:
        tables = [tables]

    # a little footgun protection
    print("Dropping tables in 5 seconds")
    time.sleep(5)

    for data_table in tables:
        print(f"Deleting table {data_table}")
        for file in os.listdir(f"{pool.data_path}/{data_table}"):
            if ".parquet" not in file:
                continue

            data = (
                pl.scan_parquet(f"{pool.data_path}/{data_table}/{file}")
                .filter(pl.col("chain_name") == pool.chain)
                .head(10)
                .collect()
            )

            if not data.is_empty():
                # rip
                os.remove(f"{pool.data_path}/{data_table}/{file}")
