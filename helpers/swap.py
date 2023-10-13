from .swap_math import *


def parseEntry(calldata, field, required=True):
    entry = calldata.get(field, None)
    assert entry != None or required, f"Missing {field}"

    return entry


def parseCalldata(calldata):
    as_of = parseEntry(calldata, "as_of")
    tokenIn = parseEntry(calldata, "tokenIn")
    swapIn = parseEntry(calldata, "swapIn")
    findMax = parseEntry(calldata, "findMax", required=False)

    return as_of, tokenIn, swapIn, findMax


def inRangeTesting(zeroForOne, inRange0, inRangeToSwap0, inRange1, inRangeToSwap1):
    # is there enough liquidity in the current tick?
    if zeroForOne:
        inRangeTest, inRangeToSwap = inRange0, inRangeToSwap0
    else:
        inRangeTest, inRangeToSwap = inRange1, inRangeToSwap1

    return inRangeTest, inRangeToSwap


def swapIn(calldata, pool):
    """
    calldata = {'as_of': 104043220,
            'tokenIn': pool.token1,
            'swapIn': 5_000_000 * 1e6,
            'findMax': False}


    swapIn(calldata, pool)
    """
    as_of, tokenIn, swapIn, findMax = parseCalldata(calldata)

    if as_of != pool.cache["as_of"]:
        pool.calcSwapDF(as_of)

    swap_df, inRangeValues = pool.cache["swapDF"], pool.cache["inRangeValues"]

    zeroForOne = True
    assetIn, assetOut = "x", "y"

    if tokenIn == pool.token1:
        zeroForOne = False
        assetIn, assetOut = "y", "x"

    # unpack these values
    (
        sqrt_P,
        inRange0,
        inRangeToSwap0,
        inRange1,
        inRangeToSwap1,
        liquidity_in_range,
        tick_in_range,
    ) = inRangeValues

    inRangeTest, inRangeToSwap = inRangeTesting(
        zeroForOne, inRange0, inRangeToSwap0, inRange1, inRangeToSwap1
    )

    # we want to force the ticks to shift
    if findMax:
        swapInMinusFee = inRangeTest

    swapInMinusFee = swapIn * (1 - pool.fee / 1e6)

    if inRangeTest > swapInMinusFee:
        # enough liquidity in range
        liquidity = liquidity_in_range

        # determine how far to push in-range
        if not zeroForOne:
            sqrtPriceLast = get_next_price_amount1(
                sqrt_P, liquidity, swapInMinusFee, zeroForOne
            )
            amtOut = get_amount0_delta(sqrtPriceLast, sqrt_P, liquidity)
        else:
            sqrtPriceLast = get_next_price_amount0(
                sqrt_P, liquidity, swapInMinusFee, zeroForOne
            )
            amtOut = get_amount1_delta(sqrtPriceLast, sqrt_P, liquidity)

    # we gotta shift tick(s) lol
    else:
        """
        we did not find enough liquidity in the current tick to trade in, so now we have to shift ticks

        in solidity, v3 continually loops until we exhaust the amount to trade (thus will become gas intensive)
        when there is not enough liquidity in the pool)

        however here, we vectorize precompute every single tick possible to move over and then find the tick
        cumulatively that has enough for us to swap into
        """
        leftToSwap = swapInMinusFee - inRangeTest

        # we precompute all possible ticks
        oor = (
            swap_df.filter(
                (
                    pl.col("tick") < tick_in_range
                    if zeroForOne
                    else pl.col("tick") > tick_in_range
                )
            )
            .sort(pl.col("tick"), descending=zeroForOne)
            .with_columns(
                cumulativeX=pl.col("xInTick").cumsum(),
                cumulativeY=pl.col("yInTick").cumsum(),
            )
        )

        assetColumn = pl.col("cumulativeX") if zeroForOne else pl.col("cumulativeY")

        maxAmountOut = oor.select(assetColumn).max().item()

        if findMax:
            pass
        assert maxAmountOut > leftToSwap, "Not enough liquidity in pool"

        # this is the tick that has cumulatively enough liquidity to support
        # our entire trade
        liquidTickRow = oor.filter(assetColumn >= leftToSwap).head(1)

        liquidTick = liquidTickRow["tick"].item()

        # find the previous ticks
        previousTicks = oor.filter(
            pl.col("tick") > liquidTick if zeroForOne else pl.col("tick") < liquidTick
        )

        sqrt_P_last_top, sqrt_P_last_bottom = (
            liquidTickRow["p_b"].item(),
            liquidTickRow["p_a"].item(),
        )

        liquidity = liquidTickRow["liquidity"].item()

        amtInToSwapLeft = leftToSwap - previousTicks[f"{assetIn}InTick"].sum()
        # fee support goes here
        amtInSwappedLeftMinusFee = amtInToSwapLeft * (1 - pool.fee / 1e6)
        amtOutPrevTicks = inRangeToSwap + previousTicks[f"{assetOut}InTick"].sum()

        amtOutLastTick, sqrtPriceLast = finalAmtOutFromTick(
            zeroForOne,
            sqrt_P_last_top,
            sqrt_P_last_bottom,
            amtInSwappedLeftMinusFee,
            liquidity,
        )

        amtOut = amtOutLastTick + amtOutPrevTicks

    return amtOut, sqrtPriceLast
