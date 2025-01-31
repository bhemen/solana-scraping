import requests
import os
from pathlib import Path
import csv
import pandas as pd

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
url = f"{base_url}?address={address}"

headers = {
    "accept": "application/json",
    "x-chain": "solana",
    "X-API-KEY": api_key
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    print(response.text)
else:
    print(f"Error: {response.status_code}")
    print(response.text)
