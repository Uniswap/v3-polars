# v3-polars
 
## Uniswap v3 simulator
### Features 
1. Maintain mints/burns/initializes/factories/swaps on all required chains
2. Creates helpers for reading Uniswap v3 data
3. Implements Optimism OVM1 -> EVM migration seemlessly
4. Allows swap simulating
5. Creates liquidity distributions
6. Historical price helpers

### GIF created using v3-polars
![](./assets/animation.gif)

### Data Providers
If you are an academic writing a paper, feel free to reach out to <a href="https://twitter.com/AustinAdams10">Austin</a>

#### Allium
The wonderful folks at <a href ="https://allium.so/">Allium</a> have integrated with v3-polars as part of their <a href="https://x.com/UniswapFND/status/1776002168681529549">DEX Analytics Portal grant</a>

You'll need to configure the authentication with Allium. To do that:

If you have access to the Allium app:
1. Login to app.allium.so and create an explorer query
2. Place only {{query_text}} in the query
3. Click "save" on the top right, then "export to api"
4. Here, you will get the query id and api key

If you do not have access to the Allium app, email <dexanalytics@allium.so>

After you get credentials, pass them to the ALLIUM_POLARSV3_QUERY_ID and ALLIUM_POLARSV3_API_KEY environment variables

Pro tip: you can use python to set the env variables:

```python
import os

os.environ['ALLIUM_POLARSV3_QUERY_ID'] = 'abcdefg'
os.environ['ALLIUM_POLARSV3_API_KEY'] = 'deadbeef'

# add the rest of your code here, like:
# state.v3Pool(...)
```

#### Google BigQuery
To use the GBQ database, you need to be auth'd with Uniswap Labs backend. 
Likely, if you don't know how to do this, then you should use another provider.

However, if you think you should be auth'd and are not, run the code below

```bash
gcloud auth login
```

To solve "GCP could not be imported" errors, try installing the bigquery library
```bash
pip install google-cloud-bigquery
```

#### Your provider
Create a new connector in v3/helpers/connectors using template.py.
Integrate your connector into data_update.py under update_tables and make a PR!

### Simple examples

Pull and then read all ETH/USDC swaps on Arbitrum
```python
address = '0xc31e54c7a869b9fcbecc14363cf510d1c41fa443'

# if you're using allium, pass update_from='allium' to v3Pool()
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
