import requests
import os
from pathlib import Path
import csv
import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime

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

def get_price_history( address, start_ts, end_ts, retry=0, wait=5):
    base_url = f"https://public-api.birdeye.so/defi/history_price"
    period = "1D"
    url = f"{base_url}?address={address}&address_type=token&type={period}&time_from={int(start_ts)}&time_to={int(end_ts)}"

    headers = {
        "accept": "application/json",
        "x-chain": "solana",
        "X-API-KEY": api_key
    }

    try:
        response = requests.get(url, headers=headers)
    except Exception as e:
        with open( errfile, "a" ) as f:
            f.write(f"{address},{e},{url}\n")
        if retry < max_retries:
            time.sleep( wait )
            return get_price_history( address, start_ts, end_ts, retry+1, wait*2 )
        else:
            return None

    if response.status_code == 200:
        try:
            json_response = response.json()
            if 'data' in json_response and 'items' in json_response['data']:
                df = pd.DataFrame(json_response['data']['items'])
                df.rename( columns={
                    'unixTime': 'ts',
                    'value': 'price'
                }, inplace=True )
                #df['tokenAddress'] = address
                return df
            return None

        except Exception as e:
            with open( errfile, "a" ) as f:
                f.write(f"{address},{e},{url}\n")
            if retry < max_retries:
                time.sleep( wait )
                return get_price_history( address, start_ts, end_ts, retry+1, wait*2 )
            else:
                return None
        #{"data":{"txHash":"3cW2HpkUs5Hg2FBMa52iJoSMUf8MNkkzkRcGuBs1JEesQ1pnsvNwCbTmZfeJf8hTi9NSHh1Tqx6Rz5Wrr7ePDEps","slot":223012712,"tokenAddress":"D7rcV8SPxbv94s3kJETkrfMrWqHFs6qrmtbiu6saaany","decimals":5,"owner":"JEFL3KwPQeughdrQAjLo9o75qh15nYbFJ2ZDrb695qsZ","blockUnixTime":1697044029,"blockHumanTime":"2023-10-11T17:07:09.000Z"},"success":true}
    else:
        with open( errfile, "a" ) as f:
            f.write(f"{address},{response.status_code},{url},{response.text}\n")
        if retry < max_retries:
            time.sleep( wait )
            return get_price_history( address, start_ts, end_ts, retry+1, wait*2 )
        else:
            return None

def get_token_launch_ts( address ):
    try:
        df = pd.read_csv( f"data/token_creation_info.csv" )
        df = df[ df.tokenAddress == address ]
        return int(df.blockUnixTime.values[0])
    except Exception as e:
        return None

def get_timestamps( address ):
    outfile = f"data/trading_history_{address}.csv"
    try:
        df = pd.read_csv( outfile )
        earliest_ts = df.ts.min()
        latest_ts = df.ts.max()
    except Exception as e:
        earliest_ts = None
        latest_ts = None

    return earliest_ts, latest_ts

for address in tqdm(set(token_addresses).difference(completed_addresses)):

    outfile = f"data/trading_history_{address}.csv"

    tqdm.write( f"Getting trading history for {address}" )
    earliest_ts, latest_ts = get_timestamps( address )
    if earliest_ts is None:
        earliest_ts = datetime( 2024, 1, 1 ).timestamp()
    if latest_ts is None:
        latest_ts = datetime.now().timestamp()


    period = 7 * 24 * 60 * 60
    start_ts = latest_ts
    launch_ts = get_token_launch_ts( address )
    if launch_ts is not None:
        start_ts = max(start_ts,launch_ts)
    end_ts = start_ts + period

    while start_ts < datetime.now().timestamp():
        trading_history = get_price_history( address, start_ts, end_ts )
        if trading_history is None:
            continue

        if Path( outfile ).is_file():
            with open( outfile, "a" ) as f:
                dw = csv.DictWriter( f, trading_history.columns )
                for rd in trading_history.to_dict( orient='records' ):
                    dw.writerow( rd )
        else:
            with open( outfile, "w" ) as f:
                dw = csv.DictWriter( f, trading_history.columns )
                dw.writeheader()
                for rd in trading_history.to_dict( orient='records' ):
                    dw.writerow( rd )

        start_ts = end_ts
        end_ts = start_ts + period
        time.sleep( 1 )


