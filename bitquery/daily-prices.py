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
import argparse

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

# Parse command line arguments
parser = argparse.ArgumentParser(description='Scrape token trades from Bitquery API')
parser.add_argument('--descfield', default='volume', help='Field to sort by (default: volume)')
parser.add_argument('--protocol', default='all', help='Protocol name to filter by (default: all)')
parser.add_argument('--numrecords', default='4000', help='Total number of records to scrape (default: 4000)')
args = parser.parse_args()

DESCFIELD = args.descfield
PROTOCOLNAME = args.protocol
NUMRECORDS = int(args.numrecords)

try:
    with open(f'{dir_path}/api_key', 'r') as file:
        api_key = file.read().rstrip()
except Exception as e:
    api_key = None
    print( e )

try:
    with open(f'{dir_path}/access_token', 'r') as file:
        access_token = file.read().rstrip()
except Exception as e:
    access_token = None
    print( e )
  
v1_endpoint = "https://graphql.bitquery.io"
v2_endpoint = "https://streaming.bitquery.io/graphql"
eap_endpoint = "https://streaming.bitquery.io/eap"

#https://docs.bitquery.io/docs/examples/Solana/Solana-Raydium-DEX-API/#latest-price-of-a-token
with open(f'{dir_path}/base_query.gql', 'r') as file:
    base_query = file.read()

dex_query = base_query.replace('DESCFIELD', f'"{DESCFIELD}"')
if (PROTOCOLNAME != 'all') and (PROTOCOLNAME != 'any'):
    dex_query = dex_query.replace('PROTOCOLNAME', f'is: "{PROTOCOLNAME}"')
else:
    dex_query = dex_query.replace('PROTOCOLNAME', '')

print( dex_query )

batch_size = 100
today = datetime.today().strftime('%Y-%m-%d')
outfile = f"{dir_path}/data/{PROTOCOLNAME}_by_{DESCFIELD}_{today}.csv"

print( f"\nWriting to {outfile}" )

columns = ['Date', 'Dex', 'symbol', 'TokenAddress', 'count', 'lowPrice', 'highPrice', 'medianPrice', 'lowAmount', 'highAmount', 'medianAmount', 'volume']

with open(outfile, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=columns)
    writer.writeheader()

rows = []
for i in tqdm( range( NUMRECORDS//batch_size ) ):
    variables = {   'OFFSET': i*batch_size, 
                    'COUNT': batch_size }

    query = replace_vars( dex_query, variables )
    resp = run_query(query )
    time.sleep(5)
    d = resp['data']['Solana']['DEXTradeByTokens']
    if d is None:
        continue
    with open(outfile, 'a', newline='') as csvfile:
        for r in d:
            try:
                row = { 'Date': r['Block']['datefield'], 'Dex': r['Trade']['Dex']['ProtocolName'], 'symbol': r['Trade']['Currency']['Symbol'], 'TokenAddress': r['Trade']['Currency']['MintAddress'], 'count': r['tradeCount'],'lowPrice': r['lowPrice'],'highPrice': r['highPrice'], 'medianPrice': r['medPrice'], 'lowAmount': r['lowAmt'],'highAmount': r['highAmt'], 'medianAmount': r['medAmt'], 'volume': r['volume']  }
                writer = csv.DictWriter(csvfile, fieldnames=row.keys())
                writer.writerow( row )
            except Exception as e:
                print( f"Error writing row to {outfile}" )
                print( e )


