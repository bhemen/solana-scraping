# Bitquery data

[Bitquery provides access to Solana data](https://docs.bitquery.io/docs/examples/Solana/solana-dextrades/) through a GraphQL interface.
You'll need to sign up for an API key, and place it in a file called `api_key' in order to run these scripts.

# Scripts

* [daily-prices.py](daily-prices.py) queries *all* DEX trades on Solana *on the current day.*  Sadly, the Bitquery API will not let you query historical data.
	If you set [daily-prices.py](daily-prices.py) running as a cron job, you can get daily trade data (one day per file, as in [data/all_by_volume_2024-12-12.csv](data/all_by_volume_2024-12-12.csv)).
* [get_dexes.py](get_dexes.py) uses the Bitquery API to get a list of all Solana DEXes indexed by Bitquery.  The output is written to [data/DEXes.csv](data/DEXes.csv).

