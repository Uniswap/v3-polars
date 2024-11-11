import numpy as np
import math
import polars as pl


# math functions
def priceX96ToTick(price):
    """
    Helper to convert sqrtPriceX96 to the current non-int tick of the pool
    """
    Q96 = 2**96

    return np.log((price / Q96) ** 2) / np.log(1.0001)


def priceX96ToTickFloor(price, ts):
    """
    Helper to convert sqrtPriceX96 to the current integer tick spacing of the pool
    """
    tick = priceX96ToTick(price)

    return (int(math.floor(tick)) // ts) * ts


def createLiq(bn, pool, data, data_path):
    """
    This is very complicated but

    1. it groups all the mints/burns on the same lower tick
    2. groups all the mints/burns on the same upper tick
    and inverts the liquidity price
    3. combines the liquidity at the same tick
    4. the cumsums

    """
    tl = (
        pool.readFromMemoryOrDisk(data, data_path)
        .with_columns(
            liquidity_lower=(pl.col("amount") * pl.col("type_of_event")),
            as_of=pl.col("block_number") + pl.col("transaction_index") / 1e4,
        )
        .filter(pl.col("as_of") < bn)
        .group_by("tick_lower")
        .agg(pl.col("liquidity_lower").sum())
        .filter(pl.col("liquidity_lower") != 0)
        .rename({"tick_lower": "tick"})
    )

    tu = (
        pool.readFromMemoryOrDisk(data, data_path)
        .with_columns(
            liquidity_upper=(-1 * (pl.col.amount * pl.col.type_of_event)),
            as_of=pl.col("block_number") + pl.col("transaction_index") / 1e4,
        )
        .filter(pl.col("as_of") < bn)
        .group_by("tick_upper")
        .agg(pl.col("liquidity_upper").sum())
        .filter(pl.col("liquidity_upper") != 0)
        .rename({"tick_upper": "tick"})
    )

    liquidity_distribution = (
        tl.join(tu, on="tick", how="outer")
        .fill_null(0)
        .with_columns(liquidity=(pl.col("liquidity_lower") + pl.col("liquidity_upper")))
        .sort(pl.col("tick"))
        .select(["tick", "liquidity"])
        .with_columns(liquidity=(pl.col("liquidity").cum_sum()))
    )

    return liquidity_distribution


def finalAmtOutFromTick(
    zeroForOne, sqrt_P_last_top, sqrt_P_last_bottom, amtInSwappedLeftMinusFee, liquidity
):
    """
    See https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/SwapMath.sol
    Specficially computeSwapStep()
    """
    if zeroForOne:
        sqrtP_next = get_next_price_amount0(
            sqrt_P_last_top, liquidity, amtInSwappedLeftMinusFee, zeroForOne
        )

        amtOutTick = get_amount1_delta(sqrtP_next, sqrt_P_last_top, liquidity)

    else:
        sqrtP_next = get_next_price_amount1(
            sqrt_P_last_bottom,
            liquidity,
            amtInSwappedLeftMinusFee,
            zeroForOne,
        )

        amtOutTick = get_amount0_delta(sqrtP_next, sqrt_P_last_bottom, liquidity)

    return amtOutTick, sqrtP_next


def get_amount0_delta(ratioA, ratioB, liq):
    """
    See https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/SqrtPriceMath.sol
    """
    if ratioA > ratioB:
        ratioA, ratioB = ratioB, ratioA

    return liq * ((ratioB - ratioA) / (ratioB * ratioA))


def get_amount1_delta(ratioA, ratioB, liq):
    """
    See https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/SqrtPriceMath.sol
    """
    if ratioA > ratioB:
        ratioA, ratioB = ratioB, ratioA
    return liq * (ratioB - ratioA)


def get_next_price_amount0(ratioA, liq, amount, add):
    """
    See https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/SqrtPriceMath.sol
    """
    if add:
        sqrtPrice_trade = (liq * ratioA) / (liq + amount * ratioA)
    else:
        sqrtPrice_trade = (liq * ratioA) / (liq - amount * ratioA)

    return sqrtPrice_trade


def get_next_price_amount1(ratioA, liq, amount, add):
    """
    See https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/SqrtPriceMath.sol
    """
    if not add:
        sqrtPrice_trade = ratioA + amount / (liq)
    else:
        sqrtPrice_trade = ratioA - amount / (liq)

    return sqrtPrice_trade


def get_next_sqrtPrice(ratioA, liq, amount, zeroForOne):
    """
    See https://github.com/Uniswap/v3-core/blob/main/contracts/libraries/SqrtPriceMath.sol
    """
    if zeroForOne:
        sqrtPrice_next = get_next_price_amount0(ratioA, liq, amount, zeroForOne)
    else:
        sqrtPrice_next = get_next_price_amount1(ratioA, liq, amount, zeroForOne)

    return sqrtPrice_next
