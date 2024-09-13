#!/usr/bin/python3
"""
    Scrape all token trades on the current day from the Bitquery API 
    Saves the data to the CSV data/token_prices_{current_date}.csv
"""
import requests 
import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime
import os 

#If we call this from cron, it will run it from a different CWD, so relative paths won't work
dir_path = os.path.dirname(os.path.realpath(__file__))

with open(f'{dir_path}/api_key', 'r') as file:
    api_key = file.read().replace('\n', '')
  
v1_endpoint = "https://graphql.bitquery.io"
v2_endpoint = "https://streaming.bitquery.io/graphql"
eap_endpoint = "https://streaming.bitquery.io/eap"

#https://docs.bitquery.io/docs/examples/Solana/Solana-Raydium-DEX-API/#latest-price-of-a-token
dex_query = """ 
query MyQuery {
  Solana {
    DEXTradeByTokens(
      orderBy: {descendingByField: "volume"}
      limit: {count: COUNT, offset: OFFSET}
    ) {
      Block {
        datefield: Date(interval: {in: days, count: 1 })
      }
      volume: sum(of: Trade_AmountInUSD)
      med: median(of: Trade_AmountInUSD)
      low: quantile(of: Trade_AmountInUSD, level: 0.025)
      high: quantile(of: Trade_AmountInUSD, level: 0.975)
      Trade {
        Currency {
          Symbol
          MintAddress
        }
        Dex {
          ProtocolName
        }
      }
      count
    }
  }
}
"""

def replace_vars( query, var_dict ):
    q = query
    for k,v in var_dict.items():
        q = q.replace( k, str(v) )
    return q

def run_query(query):
    headers = {'X-API-KEY': api_key}
    request = requests.post(eap_endpoint, json={'query': query }, headers=headers)
    if request.status_code == 200:
        return request.json()
    elif request.status_code == 429: #Too many request
        time.sleep(10)
        return run_query(query)
    else:
        raise Exception('Query failed and return code is {}.    {}'.format(request.status_code, query))

num_records = 2000
batch_size = 5
today = datetime.today().strftime('%Y-%m-%d')
outfile = f"/{dir_path}/data/token_prices_{today}.csv"

rows = []
for i in tqdm( range( num_records//batch_size ) ):
    variables = {   'OFFSET': i*batch_size, 
                    'COUNT': batch_size }

    query = replace_vars( dex_query, variables )
    resp = run_query(query )
    time.sleep(5)
    d = resp['data']['Solana']['DEXTradeByTokens']
    for r in d:
        row = { 'Date': r['Block']['datefield'], 'Dex': r['Trade']['Dex']['ProtocolName'], 'symbol': r['Trade']['Currency']['Symbol'], 'TokenAddress': r['Trade']['Currency']['MintAddress'], 'count': r['count'], 'high': r['high'], 'median': r['med'], 'volume': r['volume']  }
        rows.append( row )

print( f"\nWriting to {outfile}" )
df = pd.DataFrame( rows )
df.to_csv( outfile, index=False )


