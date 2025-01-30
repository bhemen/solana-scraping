import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import csv
import time

def get_pump_metadata(token_address,retry=0,backoff=10):
    url = f"https://frontend-api.pump.fun/coins/{token_address}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            json = response.json()
            return json
        else:
            if retry < max_retries:
                tqdm.write(f"Failed to get pump metadata from {url}, retrying...")
                time.sleep(backoff)
                return get_pump_metadata(token_address, retry + 1, backoff*2)
            else:
                tqdm.write( response.text )
                raise Exception(f"Failed to get pump metadata for {token_address}")
    except Exception as e:
        print(f"Error getting pump metadata for {token_address}: {e}")
        return None

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

outfile = "data/pump_metadata.csv"

known_addresses = []
try:
    df = pd.read_csv(outfile)
    known_addresses = df.TokenAddress.unique()
except Exception as e:
    pass

addresses_to_get = list(set(token_addresses).difference(known_addresses))
max_retries = 5

for address in tqdm(addresses_to_get):
    if not address.endswith('pump'):
        continue
    try:
        metadata = get_pump_metadata(address)
    except Exception as e:
        tqdm.write(f"Error getting pump metadata for {address}: {e}")
        continue
    if metadata:
        if Path(outfile).is_file():
            with open(outfile, 'a') as f:
                csv_writer = csv.DictWriter(f, fieldnames=metadata.keys())
                csv_writer.writerow(metadata)
        else:
            with open(outfile, 'w') as f:
                csv_writer = csv.DictWriter(f, fieldnames=metadata.keys())
                csv_writer.writeheader()
                csv_writer.writerow(metadata)
    time.sleep(5)
