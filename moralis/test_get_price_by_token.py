#!/usr/bin/env python3
import sys
from datetime import datetime
from moralis_utils import get_price_by_token, QuotaExceededException

# Test with Luigi token
token_address = "5XyKkFaJpAmsH4Tf2EFj3S61W3hC5cJhxNZQQ5h1pump"
start_date = datetime(2024, 12, 9)

print(f"Testing get_price_by_token for token: {token_address}")
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
