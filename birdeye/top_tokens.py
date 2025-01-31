import requests
import os
from pathlib import Path
import csv
import pandas as pd

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
base_url = "https://public-api.birdeye.so/defi/token_trending"
url = f"{base_url}?sort_by=rank&sort_type=desc&offset={offset}&limit={limit}"


response = requests.get(url, headers=headers)

print(response.text)