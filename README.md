# v3-polars
 
## Uniswap v3 simulator
### Features 
1. Maintain mints/burns/initalizes/factories/swaps on all required chains
2. Creates helpers for reading Uniswap v3 data
3. Impliments Optimism OVM1 -> EVM migration seemlessly
4. Allows swap simulating
5. Creates liquidity distributions
6. Historical price helpers

### GIF created using v3-polars
![](./assets/animation.gif)
### Simple examples


Pull and then read all ETH/USDC swaps on Arbitrum
```python
address = '0xc31e54c7a869b9fcbecc14363cf510d1c41fa443'
arb = state.v3Pool(address, 'arbitrum', update = True)

swaps = arb.getSwaps
```


Working off the previous example, get the price of the arbitrum pool
every 15 minutes
```python
priceArb = arb.getPriceSeries(starting, frequency = '15m', gas = True)
```


Calculate the output a 1 ETH swap at block 150000000
```python
calldata = {'as_of': 150000000,
        'tokenIn': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
        'swapIn': 1e18}

amt, _ = arb.swapIn(calldata)
```
