import requests
import os
from pathlib import Path
import csv
import pandas as pd
from tqdm import tqdm
import time

data_path = Path("../bitquery/data")

csv_files = list(data_path.glob("token_prices*.csv"))
token_addresses = set()
for f in csv_files:
    df = pd.read_csv( f )
    if 'TokenAddress' in df.columns:
        token_addresses = token_addresses.union( df.TokenAddress )

with open( "data/token_list.csv", "w" ) as f:
    csv_writer = csv.writer(f)
    for address in token_addresses:
        csv_writer.writerow([address])

dir_path = os.path.dirname(os.path.realpath(__file__))
try:
    with open(f'{dir_path}/api_key', 'r') as file:
        api_key = file.read().rstrip()
except Exception as e:
    api_key = None
    print( e )


address = "D7rcV8SPxbv94s3kJETkrfMrWqHFs6qrmtbiu6saaany"
base_url = f"https://public-api.birdeye.so/defi/token_creation_info"
outfile = "data/token_creation_info.csv"
errfile = "birdeye_errors.csv"
max_retries = 3

if os.path.exists( outfile ):
    df = pd.read_csv( outfile )
    completed_addresses = set( df.tokenAddress )
else:
    completed_addresses = set()

def get_token_creation_info( address, retry=0, wait=5):
    url = f"{base_url}?address={address}"

    headers = {
        "accept": "application/json",
        "x-chain": "solana",
        "X-API-KEY": api_key
    }

    try:
        response = requests.get(url, headers=headers)
    except Exception as e:
        with open( errfile, "a" ) as f:
            f.write(f"{address},{e}\n")
        if retry < max_retries:
            time.sleep( wait )
            return get_token_creation_info( address, retry+1, wait*2 )
        else:
            return None

    if response.status_code == 200:
        try:
            json_response = response.json()
            return json_response['data']
        except Exception as e:
            with open( errfile, "a" ) as f:
                f.write(f"{address},{e}\n")
            if retry < max_retries:
                time.sleep( wait )
                return get_token_creation_info( address, retry+1, wait*2 )
            else:
                return None
        #{"data":{"txHash":"3cW2HpkUs5Hg2FBMa52iJoSMUf8MNkkzkRcGuBs1JEesQ1pnsvNwCbTmZfeJf8hTi9NSHh1Tqx6Rz5Wrr7ePDEps","slot":223012712,"tokenAddress":"D7rcV8SPxbv94s3kJETkrfMrWqHFs6qrmtbiu6saaany","decimals":5,"owner":"JEFL3KwPQeughdrQAjLo9o75qh15nYbFJ2ZDrb695qsZ","blockUnixTime":1697044029,"blockHumanTime":"2023-10-11T17:07:09.000Z"},"success":true}
    else:
        with open( errfile, "a" ) as f:
            f.write(f"{address},{response.status_code}\n")
        if retry < max_retries:
            time.sleep( wait )
            return get_token_creation_info( address, retry+1, wait*2 )
        else:
            return None

for address in tqdm(set(token_addresses).difference(completed_addresses)):
    info = get_token_creation_info( address )

    if Path( outfile ).is_file():
        with open( outfile, "a" ) as f:
            dw = csv.DictWriter( f, info.keys() )
            dw.writerow( info )
    else:
        with open( outfile, "w" ) as f:
            dw = csv.DictWriter( f, info.keys() )
            dw.writeheader()
            dw.writerow( info )
    
    time.sleep( 1 )


