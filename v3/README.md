The function `readFromMemoryOrDisk` intakes a parquet (as components of a filepath) and outputs a Polars dataframe. After running the function, here is the schema for `pool_mint_burn_events` and `pool_swap_events` dataframes.

**Swaps**
| Column            | Polars datatype |
|-------------------|-----------------|
|     chain_name    | str             |
|      address      | str             |
|  block_timestamp  | datetime[MiS]   |
|    block_number   | i64             |
|  transaction_hash | str             |
|       sender      | str             |
|     recipient     | str             |
|      amount0      | f64             |
|      amount1      | f64             |
|    sqrtPriceX96   | f64             |
|     liquidity     | f64             |
|        tick       | i32             |
|    from_address   | str             |
|     to_address    | str             |
| transaction_index | i64             |
|     gas_price     | i64             |
|      gas_used     | i64             |
|       l1_fee      | i64             |

**Mint & Burn**
| Column            | Polars datatype |
|-------------------|-----------------|
|     chain_name    | str             |
|      address      | str             |
|  block_timestamp  | datetime[us]    |
|    block_number   | i64             |
|  transaction_hash | str             |
|     log_index     | i64             |
|       amount      | f64             |
|      amount0      | f64             |
|      amount1      | f64             |
|       owner       | str             |
|     tick_lower    | i64             |
|     tick_upper    | i64             |
|   type_of_event   | f64             |
|     to_address    | str             |
|    from_address   | str             |
| transaction_index | i64             |
|     gas_price     | i64             |
|      gas_used     | i64             |
|       l1_fee      | i64             |