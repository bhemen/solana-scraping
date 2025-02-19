import requests
import os
from pathlib import Path
import csv
import pandas as pd
import json
from datetime import datetime

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

dir_path = os.path.dirname(os.path.realpath(__file__))
try:
    with open(f'{dir_path}/api_key', 'r') as file:
        api_key = file.read().rstrip()
except Exception as e:
    api_key = None
    print( e )

headers = {
    "accept": "application/json",
    "x-chain": "solana",
    "X-API-KEY": api_key
}

offset = 0
limit = 20
num_tokens = 1000

base_url = "https://public-api.birdeye.so/defi/token_trending"
url = f"{base_url}?sort_by=rank&sort_type=desc&offset={offset}&limit={limit}"
error_file = f"{dir_path}/top_tokens_errors.csv"
today = datetime.today().strftime('%Y-%m-%d')
outfile = f"{dir_path}/data/top_tokens-{today}.csv"

def get_top_tokens(offset, limit):
    url = f"{base_url}?sort_by=rank&sort_type=desc&offset={offset}&limit={limit}"
    try:
        response = requests.get(url, headers=headers)
        json_response = response.json()['data']
        return json_response
    except Exception as e:
        with open(error_file, 'a') as f:
            f.write(f"{offset},{limit},{e}\n")
        return []

for i in tqdm(range(num_tokens // limit)):
    response = get_top_tokens(offset, limit)
    ts = response['updateUnixTime']
    tokens = response['tokens']

    if Path(outfile).is_file():
        with open(outfile, 'a') as f:
            all_keys = set().union(*(t.keys() for t in tokens))
            all_keys.add('ts')
            dw = csv.DictWriter(f, all_keys)
            for token in tokens:
                row = token.copy()
                row.update( {'ts': ts} )
                dw.writerow(row)
    else:
        with open(outfile, 'w') as f:
            all_keys = set().union(*(t.keys() for t in tokens))
            all_keys.add('ts')
            dw = csv.DictWriter(f, all_keys)
            dw.writeheader()
            for token in tokens:
                row = token.copy()
                row.update( {'ts': ts} )
                dw.writerow(row)

    offset += limit
