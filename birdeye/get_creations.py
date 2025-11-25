"""
Fetch security details for all tokens from bitquery data CSVs.
Uses the BirdeyeAPI get_token_security_details function.
"""

import os
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import time
from birdeye_utils import BirdeyeAPI

# Path to bitquery data
data_path = Path("../bitquery/data")

# Find all token addresses from CSV files
csv_files = list(data_path.glob("*.csv"))
token_addresses = set()

print(f"Reading token addresses from {len(csv_files)} CSV files...")
for f in csv_files:
    try:
        df = pd.read_csv(f)
        if 'TokenAddress' in df.columns:
            token_addresses = token_addresses.union(df.TokenAddress)
    except Exception as e:
        print( f'Failed to read {f}' )
        print( e )

print(f"Found {len(token_addresses)} unique token addresses")

# Save token list
with open("data/token_list.csv", "w") as f:
    f.write("address\n")
    for address in token_addresses:
        f.write(f"{address}\n")

print(f"Saved token list to data/token_list.csv")

# Initialize API client
api = BirdeyeAPI(verbose=False)

# Check which addresses already have security details
security_csv = "data/security_details.csv"
if Path(security_csv).exists():
    df = pd.read_csv(security_csv, on_bad_lines='warn')
    completed_addresses = set(df['address'])
    print(f"Found {len(completed_addresses)} tokens with existing security details")
else:
    completed_addresses = set()
    print("No existing security details found")

# Get addresses that need processing
remaining_addresses = list(token_addresses - completed_addresses)
print(f"\nFetching security details for {len(remaining_addresses)} tokens...")

# Track results
successful = []
failed = []

# Process each address
for address in tqdm(remaining_addresses, desc="Fetching security details"):
    try:
        # Get security details (automatically saves to CSV)
        security = api.get_token_security_details(address)

        if security is not None:
            successful.append(address)
        else:
            failed.append(address)

    except Exception as e:
        tqdm.write(f"Error processing {address}: {e}")
        failed.append(address)

    # Sleep to avoid rate limits
    time.sleep(1)

# Print summary
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Total tokens: {len(token_addresses)}")
print(f"Already had security details: {len(completed_addresses)}")
print(f"Newly successful: {len(successful)}")
print(f"Failed: {len(failed)}")

if failed:
    print(f"\nFailed addresses ({len(failed)}):")
    for addr in failed[:10]:  # Show first 10
        print(f"  - {addr}")
    if len(failed) > 10:
        print(f"  ... and {len(failed) - 10} more")

print(f"\nSecurity details saved to: {security_csv}")
print("Done!")
