"""
Fetch price history for all token addresses from CSV files in ../pump/data/coin_search/
"""

from birdeye_utils import BirdeyeAPI
from pathlib import Path
import time
from datetime import datetime
from tqdm import tqdm
import argparse
import pandas as pd

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch price history for addresses in pump coin_search CSVs')
    parser.add_argument('--default-start-date', type=str, default='2024-01-01',
                       help='Default start date if creation time not found (YYYY-MM-DD)')
    parser.add_argument('--skip-missing', action='store_true',
                       help='Skip tokens without creation time instead of using default')
    parser.add_argument('--sleep-time', type=float, default=1.5,
                       help='Seconds to sleep between API requests (default: 1.5)')
    parser.add_argument('--fetch-security', action='store_true',
                       help='Also fetch security details for each token')
    parser.add_argument('--verbose', action='store_true',
                       help='Show detailed progress for each token (default: only show summary)')
    args = parser.parse_args()

    # Initialize API client with verbose=False to prevent breaking tqdm output
    api = BirdeyeAPI(verbose=False)

    # Parse default start date
    default_start_ts = None
    if not args.skip_missing:
        try:
            default_start_ts = int(datetime.strptime(args.default_start_date, '%Y-%m-%d').timestamp())
            print(f"Using default start date: {args.default_start_date} (for tokens without creation time)")
        except ValueError:
            print(f"Error: Invalid date format '{args.default_start_date}'. Use YYYY-MM-DD")
            return

    # Path to coin_search directory
    coin_search_dir = Path("../pump/data/coin_search")

    if not coin_search_dir.exists():
        print(f"Error: Directory {coin_search_dir} not found")
        return

    # Find all CSV files in the directory
    csv_files = list(coin_search_dir.glob("*.csv"))

    if not csv_files:
        print(f"Error: No CSV files found in {coin_search_dir}")
        return

    print(f"Found {len(csv_files)} CSV files in {coin_search_dir}")

    # Collect all unique addresses from all CSV files
    all_addresses = set()
    address_to_source = {}  # Track which file each address came from

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            if 'address' in df.columns:
                addresses = df['address'].dropna().unique()
                for addr in addresses:
                    addr_str = str(addr).strip()
                    if addr_str:
                        all_addresses.add(addr_str)
                        if addr_str not in address_to_source:
                            address_to_source[addr_str] = []
                        address_to_source[addr_str].append(csv_file.name)
                print(f"  {csv_file.name}: {len(addresses)} addresses")
            else:
                print(f"  Warning: {csv_file.name} has no 'address' column")
        except Exception as e:
            print(f"  Error reading {csv_file.name}: {e}")

    addresses = sorted(list(all_addresses))
    print(f"\nTotal unique addresses: {len(addresses)}")
    print(f"Starting to fetch price history for each address...")
    print()

    # Track successes, failures, skipped, and already processed
    successful = []
    failed = []
    skipped = []
    already_processed = []

    # Process each address
    for address in tqdm(addresses, desc="Processing addresses"):
        try:
            # Check if price history already exists
            output_file = f"data/price_history_{address}.csv"
            if Path(output_file).exists():
                already_processed.append(address)
                continue

            if args.verbose:
                sources = ", ".join(address_to_source[address])
                tqdm.write(f"\nProcessing {address}")
                tqdm.write(f"  Source: {sources}")

            # Fetch security details FIRST if requested (so creation time can be used)
            if args.fetch_security:
                try:
                    security = api.get_token_security_details(address)
                    if args.verbose:
                        if security is not None:
                            tqdm.write(f"  ✓ Security details retrieved")
                        else:
                            tqdm.write(f"  ⚠ Security details not available")
                except Exception as e:
                    if args.verbose:
                        tqdm.write(f"  ⚠ Error fetching security: {e}")

            # Now get creation time (will use security_details.csv if available)
            creation_time = api._get_token_creation_time(address)

            # Determine start_ts
            if creation_time is not None:
                start_ts = creation_time
            elif args.skip_missing:
                skipped.append(address)
                time.sleep(0.5)  # Short sleep even for skipped
                continue
            else:
                start_ts = default_start_ts

            # Get price history with explicit timestamps
            df = api.get_price_history(address, start_ts=start_ts, period="1D")

            if df is not None and len(df) > 0:
                # Save to CSV
                output_file = f"data/price_history_{address}.csv"
                df.to_csv(output_file, mode='w', header=True, index=False)
                successful.append(address)
                if args.verbose:
                    tqdm.write(f"  ✓ Success: Retrieved {len(df)} price records")
            else:
                failed.append(address)
                if args.verbose:
                    tqdm.write(f"  ✗ Failed: No data retrieved")

        except Exception as e:
            failed.append(address)
            if args.verbose:
                tqdm.write(f"  ✗ Error: {e}")

        # Sleep to avoid rate limits
        time.sleep(args.sleep_time)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"CSV files processed: {len(csv_files)}")
    print(f"Total unique addresses: {len(addresses)}")
    print(f"Already processed (skipped): {len(already_processed)}")
    print(f"Newly successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    if args.skip_missing:
        print(f"Skipped (no creation time): {len(skipped)}")
    if args.fetch_security:
        print(f"\nSecurity details saved to: data/security_details.csv")

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
