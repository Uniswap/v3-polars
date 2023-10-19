# data helpers
from .swap_math import *
import polars as pl


def initializePoolFromFactory(addr, chain, data_path):
    data_type = "factory_pool_created"

    factory = (
        pl.scan_parquet(f"{data_path}/{data_type}/*.parquet")
        .filter((pl.col("pool") == addr) & (pl.col("chain_name") == chain))
        .collect()
    )

    assert factory.shape[0] == 1, "Pool missing from factory"

    ts = int(factory["tickSpacing"].item())
    fee = int(factory["fee"].item())

    token0 = factory["token0"].item()
    token1 = factory["token1"].item()

    return ts, fee, token0, token1


def createSwapDF(as_of, pool):
    price = pool.getPriceAt(as_of)
    assert price != None, "Pool not initialized"

    tickFloor = priceX96ToTickFloor(price, pool.ts)
    liq = createLiq(as_of, pool, "pool_mint_burn_events", pool.data_path)

    swap_df = (
        liq.filter(pl.col("liquidity") > 0)  # numerical error
        .with_columns(
            p_a=(1.0001 ** pl.col("tick")) ** (1 / 2),
            p_b=(1.0001 ** (pl.col("tick") + pool.ts)) ** (1 / 2),
        )
        .with_columns(
            yInTick=pl.col("liquidity") * (pl.col("p_b") - pl.col("p_a")),
            xInTick=pl.col("liquidity")
            * ((pl.col("p_b") - pl.col("p_a")) / (pl.col("p_b") * pl.col("p_a"))),
        )
    )

    current_tick = swap_df.filter(pl.col("tick") == tickFloor)

    assert (
        current_tick.shape[0] == 1
    ), f"Missing/Duplicate in-range tick - Size of {current_tick.shape[0]}"

    sqrt_P = price / 2**96
    p_a, p_b, liquidity, tick = (
        current_tick["p_a"].item(),
        current_tick["p_b"].item(),
        current_tick["liquidity"].item(),
        current_tick["tick"].item(),
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


def readOVM(path, data_type):
    mappings = {
        old: new
        for old, new in (
            pl.read_csv(f"{path}/{data_type}/ovm_mapping.csv")
            .select(["oldaddress", "newaddress"])
            .iter_rows()
        )
    }

    return mappings
