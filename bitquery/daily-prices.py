#!/usr/bin/python3
"""
    Scrape all token trades on the current day from the Bitquery API 
    Saves the data to the CSV data/token_prices_{current_date}.csv
"""
import requests 
from tqdm import tqdm
import time
from datetime import datetime
import csv
import os
import sys

#Disable tqdm if running from cron / ipython
#https://github.com/tqdm/tqdm/issues/506
try:
	ipy_str = str(type(get_ipython()))
    if 'zmqshell' in ipy_str:
        from tqdm import tqdm_notebook as tqdm
    if 'terminal' in ipy_str:
        from tqdm import tqdm
except:
    if sys.stderr.isatty():
        from tqdm import tqdm
    else:
        def tqdm(iterable, **kwargs):
            return iterable 

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
      medAmt: median(of: Trade_AmountInUSD)
      lowAmt: quantile(of: Trade_AmountInUSD, level: 0.025)
      highAmt: quantile(of: Trade_AmountInUSD, level: 0.975)
      medPrice: median(of: Trade_PriceInUSD)
      lowPrice: quantile(of: Trade_PriceInUSD, level: 0.025)
      highPrice: quantile(of: Trade_PriceInUSD, level: 0.975)
      Trade {
        Currency {
          Symbol
          MintAddress
        }
        Dex {
          ProtocolName
        }
      }
      tradeCount: count(distinct: Transaction_Signature)
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

print( f"\nWriting to {outfile}" )

columns = ['Date', 'Dex', 'symbol', 'TokenAddress', 'count', 'lowPrice', 'highPrice', 'medianPrice', 'lowAmount', 'highAmount', 'medianAmount', 'volume']

with open(outfile, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=columns)
    writer.writeheader()

rows = []
for i in tqdm( range( num_records//batch_size ) ):
    variables = {   'OFFSET': i*batch_size, 
                    'COUNT': batch_size }

    query = replace_vars( dex_query, variables )
    resp = run_query(query )
    time.sleep(5)
    d = resp['data']['Solana']['DEXTradeByTokens']
    if d is None:
        continue
    with open('people.csv', 'w', newline='') as csvfile:
        for r in d:
            try:
                row = { 'Date': r['Block']['datefield'], 'Dex': r['Trade']['Dex']['ProtocolName'], 'symbol': r['Trade']['Currency']['Symbol'], 'TokenAddress': r['Trade']['Currency']['MintAddress'], 'count': r['tradeCount'],'lowPrice': r['lowPrice'],'highPrice': r['highPrice'], 'medianPrice': r['medPrice'], 'lowAmount': r['lowAmt'],'highAmount': r['highAmt'], 'medianAmount': r['medAmt'], 'volume': r['volume']  }
                writer = csv.DictWriter(csvfile, fieldnames=row.keys())
                writer.writerow( row )
            except Exception as e:
                print( f"Error writing row to {outfile}" )
                print( e )


