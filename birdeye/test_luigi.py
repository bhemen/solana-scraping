"""
Test script to fetch price history for all addresses in luigi.csv
"""

from birdeye_utils import BirdeyeAPI
from pathlib import Path
import time
from datetime import datetime
from tqdm import tqdm
import argparse

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch price history for addresses in luigi.csv')
    parser.add_argument('--default-start-date', type=str, default='2024-01-01',
                       help='Default start date if creation time not found (YYYY-MM-DD)')
    parser.add_argument('--skip-missing', action='store_true',
                       help='Skip tokens without creation time instead of using default')
    args = parser.parse_args()

    # Initialize API client
    api = BirdeyeAPI()

    # Parse default start date
    default_start_ts = None
    if not args.skip_missing:
        try:
            default_start_ts = int(datetime.strptime(args.default_start_date, '%Y-%m-%d').timestamp())
            print(f"Using default start date: {args.default_start_date} (for tokens without creation time)")
        except ValueError:
            print(f"Error: Invalid date format '{args.default_start_date}'. Use YYYY-MM-DD")
            return

    # Read addresses from luigi.csv
    luigi_file = Path("luigi.csv")

    if not luigi_file.exists():
        print(f"Error: {luigi_file} not found")
        return

    # Read all addresses
    with open(luigi_file, 'r') as f:
        addresses = [line.strip() for line in f if line.strip()]

    print(f"Found {len(addresses)} addresses in {luigi_file}")
    print(f"Starting to fetch price history for each address...")
    print()

    # Track successes, failures, and skipped
    successful = []
    failed = []
    skipped = []

    # Process each address
    for address in tqdm(addresses, desc="Processing addresses"):
        try:
            tqdm.write(f"\nProcessing {address}...")

            # First try to get creation time
            creation_time = api._get_token_creation_time(address)

            # Determine start_ts
            if creation_time is not None:
                start_ts = creation_time
            elif args.skip_missing:
                tqdm.write(f"  ⊘ Skipped: No creation time found")
                skipped.append(address)
                time.sleep(0.5)  # Short sleep even for skipped
                continue
            else:
                start_ts = default_start_ts
                tqdm.write(f"  ! Using default start date: {args.default_start_date}")

            # Get price history with explicit timestamps
            df = api.get_price_history(address, start_ts=start_ts, period="1D")

            if df is not None and len(df) > 0:
                # Save to CSV
                output_file = f"data/price_history_{address}.csv"
                df.to_csv(output_file, mode='w', header=True, index=False)
                successful.append(address)
                tqdm.write(f"  ✓ Success: Retrieved {len(df)} price records")
            else:
                failed.append(address)
                tqdm.write(f"  ✗ Failed: No data retrieved")

        except Exception as e:
            failed.append(address)
            tqdm.write(f"  ✗ Error: {e}")

        # Sleep to avoid rate limits
        time.sleep(1.5)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total addresses: {len(addresses)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    if args.skip_missing:
        print(f"Skipped: {len(skipped)}")

    if failed:
        print(f"\nFailed addresses:")
        for addr in failed:
            print(f"  - {addr}")

    if args.skip_missing and skipped:
        print(f"\nSkipped addresses (no creation time):")
        for addr in skipped:
            print(f"  - {addr}")

    print("\nDone!")

if __name__ == "__main__":
    main()
