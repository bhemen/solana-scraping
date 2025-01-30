import requests 
import pandas as pd
from tqdm import tqdm
import time

with open('api_key', 'r') as file:
    api_key = file.read().replace('\n', '')
  
v1_endpoint = "https://graphql.bitquery.io"
v2_endpoint = "https://streaming.bitquery.io/graphql"
eap_endpoint = "https://streaming.bitquery.io/eap"

#https://docs.bitquery.io/docs/examples/Solana/Solana-Raydium-DEX-API/#latest-price-of-a-token
dex_query = """ 
query {
  Solana {
    DEXTradeByTokens(
      limit: {count: COUNT, offset: OFFSET }
      orderBy: {descending: Block_Time}
      where: {Trade: {Dex: {ProgramAddress: {is: "PROGRAMADDRESS"}}, Currency: {MintAddress: {is: "TOKEN0"}}, Side: {Currency: {MintAddress: {is: "TOKEN1"}}}}}
    ) {
      Block {
        Time
      }
      Trade {
        Price
        PriceInUSD
      }
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

tokens = { 'dogwifhat': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
            'bonk': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',
            'bome': 'ukHH6c7mMyiWCf1b9pnWe25TSpkDDt3H5pQZgZ74J82',
            'mew': 'MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5',
            'popcat': '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr' }

variables = { 'OFFSET': 10, 'COUNT': 5, 'PROGRAMADDRESS': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', 'TOKEN0': '6D7NaB2xsLd7cauWu1wKk6KBsJohJmP2qZH9GEfVi5Ui', 'TOKEN1': 'So11111111111111111111111111111111111111112' }  
num_trades = 1000
batch_size = 100

rows = []
for symbol,address in tqdm( tokens.items(), total=len(tokens), desc=' Tokens', position=0 ):
    for i in tqdm( range( num_trades//batch_size ), desc=f' {symbol}', position=1, leave=False ):
        variables = { 'OFFSET': i*batch_size, 
                    'COUNT': batch_size, 
                     'PROGRAMADDRESS': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', 
                     'TOKEN0': address,
                     'TOKEN1': 'So11111111111111111111111111111111111111112' }  

        query = replace_vars( dex_query, variables )
        resp = run_query(query )
        time.sleep(5)
        d = resp['data']['Solana']['DEXTradeByTokens']
        for r in d:
            row = { 'ProgramAddress': variables['PROGRAMADDRESS'], 'Token0': variables['TOKEN0'], 'Token1': variables['TOKEN1'] }
            row.update( { 'BlockTime': r['Block']['Time'], 'Price': r['Trade']['Price'], 'PriceInUSD': r['Trade']['PriceInUSD'] } )
            rows.append( row )

df = pd.DataFrame( rows )
df.to_csv( "data/dex_trades.csv", index=False )


