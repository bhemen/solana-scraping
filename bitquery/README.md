# Bitquery data

[Bitquery provides access to Solana data](https://docs.bitquery.io/docs/examples/Solana/solana-dextrades/) through a GraphQL interface.
You'll need to sign up for an API key, and place it in a file called `api_key' in order to run these scripts.

# Scripts

* [daily_prices.py](daily_prices.py) queries *all* DEX trades on Solana *on the current day.*  Sadly, the Bitquery API will not let you query historical data.
	If you set [daily_prices.py](daily_prices.py) running as a cron job, you can get daily trade data (one day per file, as in [data/token_prices_2024-12-12.csv](data/token_prices_2024-12-12.csv)).
* [pump_prices.py](pump_prices.py) This gets the top tokens on [pump.fun](pump.fun) for a given day.  The output format is the same as from [daily_prices.py](daily_prices.py).  Although [daily_prices.py](daily_prices.py) gets data from *all* DEXes (including pump.fun), it only gets the top few thousand tokens.  Most pump tokens don't make it into that list, so it's useful to get the top pump tokens as well.   
	Data are stored daily in files like: [data/pump_prices_2024-12-12.csv](data/pump_prices_2024-12-12.csv).
* [get_dexes.py](get_dexes.py) uses the Bitquery API to get a list of all Solana DEXes indexed by Bitquery.  The output is written to [data/DEXes.csv](data/DEXes.csv).

