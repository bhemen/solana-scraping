#!/usr/bin/env python3
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
from moralis_utils import get_price_by_token, QuotaExceededException

# Test with Luigi token
token_address = "5XyKkFaJpAmsH4Tf2EFj3S61W3hC5cJhxNZQQ5h1pump"
start_date = datetime(2024, 12, 9)

# Check if data collection has already started
data_dir = Path('data')
consolidated_file = data_dir / f'price_history_{token_address}.csv'

if consolidated_file.exists():
    print(f"Found existing data file: {consolidated_file}")
    try:
        existing_df = pd.read_csv(consolidated_file)
        if not existing_df.empty and 'timestamp' in existing_df.columns:
            existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
            last_date = existing_df['timestamp'].max()
            print(f"Last date in existing data: {last_date}")

            # Use the later of start_date and last_date
            if last_date > start_date:
                start_date = last_date
                print(f"Resuming from last date: {start_date}")
            else:
                print(f"Last date is before start_date, using start_date: {start_date}")
        else:
            print("Existing file is empty or missing timestamp column, using original start_date")
    except Exception as e:
        print(f"Error reading existing data file: {e}")
        print(f"Will use original start_date: {start_date}")
else:
    print(f"No existing data file found, starting fresh")

print(f"\nTesting get_price_by_token for token: {token_address}")
print(f"Start date: {start_date}")
print("=" * 80)

try:
    results = get_price_by_token(
        token_address=token_address,
        start_date=start_date,
        delay_between_requests=2.0  # 2 second delay between pair requests
    )

    print("\n" + "=" * 80)
    print(f"Summary:")
    print(f"Total pairs processed: {len(results)}")
    for pair_address, df in results.items():
        print(f"  - {pair_address}: {len(df)} records")

except QuotaExceededException as e:
    print("\n" + "=" * 80)
    print("Script stopped due to API quota limit.")
    print("Data collected so far has been saved to CSV files.")
    print("Please wait for your quota to reset or upgrade your plan.")
    print("=" * 80)
    sys.exit(1)
