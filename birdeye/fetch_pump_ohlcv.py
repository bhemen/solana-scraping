"""
Fetch OHLCV (Open, High, Low, Close, Volume) history for all token addresses
from CSV files in ../pump/data/coin_search/

Multi-resolution fetching:
- First 24 hours: 1-minute intervals (captures early activity)
- Hours 24-192 (days 1-8): 1-hour intervals
- After day 8: 12-hour intervals
"""

from birdeye_utils import BirdeyeAPI
from pathlib import Path
import time
from datetime import datetime
from tqdm import tqdm
import argparse
import pandas as pd
import json
from typing import Optional, Tuple

# Progress tracking file
PROGRESS_FILE = "data/ohlcv_progress.json"


def load_progress() -> dict:
    """Load progress from tracking file."""
    if Path(PROGRESS_FILE).exists():
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return {"completed": [], "failed": [], "skipped": []}
    return {"completed": [], "failed": [], "skipped": []}


def save_progress(progress: dict):
    """Save progress to tracking file."""
    Path("data").mkdir(exist_ok=True)
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f)
    except Exception as e:
        print(f"Warning: Could not save progress: {e}")


def safe_api_call(api: BirdeyeAPI, address: str, start_ts: int, end_ts: int,
                  interval: str, max_retries: int = 2) -> Optional[pd.DataFrame]:
    """
    Make an API call with additional safety wrapping.

    Args:
        api: BirdeyeAPI instance
        address: Token address
        start_ts: Start timestamp
        end_ts: End timestamp
        interval: OHLCV interval
        max_retries: Max retries for this specific call

    Returns:
        DataFrame or None
    """
    for attempt in range(max_retries + 1):
        try:
            df = api.get_ohlcv_history(address, start_ts=start_ts, end_ts=end_ts, interval=interval)
            return df
        except Exception as e:
            if attempt < max_retries:
                time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                continue
            return None
    return None


def fetch_multi_resolution_ohlcv(
    api: BirdeyeAPI,
    address: str,
    creation_time: int,
    sleep_time: float = 1.5,
    verbose: bool = False
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Fetch OHLCV data at multiple resolutions based on time since launch.

    - First 24 hours: 1-minute intervals
    - Hours 24-192 (days 1-8): 1-hour intervals
    - After day 8 until now: 12-hour intervals

    Args:
        api: BirdeyeAPI instance
        address: Token address
        creation_time: Token creation timestamp (Unix seconds)
        sleep_time: Sleep between API requests
        verbose: Print detailed progress

    Returns:
        Tuple of (DataFrame or None, status_message)
    """
    now = int(datetime.now().timestamp())
    all_dfs = []
    phases_attempted = 0
    phases_succeeded = 0

    # Time boundaries (in seconds)
    HOUR = 3600
    DAY = 24 * HOUR

    end_24h = creation_time + DAY                    # 24 hours after launch
    end_8d = creation_time + (8 * DAY)               # 8 days after launch

    # Phase 1: First 24 hours at 1-minute intervals
    # API limit is 1000 records, 24h = 1440 minutes, so split into 2 requests
    if now > creation_time:
        # First 12 hours (720 minutes)
        phase1_mid = creation_time + (12 * HOUR)
        phase1_end = min(end_24h, now)

        # Request 1: hours 0-12
        phases_attempted += 1
        try:
            df1 = safe_api_call(api, address, creation_time, min(phase1_mid, now), "1m")
            if df1 is not None and len(df1) > 0:
                df1['interval'] = '1m'
                all_dfs.append(df1)
                phases_succeeded += 1
        except Exception as e:
            if verbose:
                tqdm.write(f"  Phase 1a error for {address}: {e}")

        time.sleep(sleep_time)

        # Request 2: hours 12-24 (if we're past 12h)
        if now > phase1_mid:
            phases_attempted += 1
            try:
                df2 = safe_api_call(api, address, phase1_mid, phase1_end, "1m")
                if df2 is not None and len(df2) > 0:
                    df2['interval'] = '1m'
                    all_dfs.append(df2)
                    phases_succeeded += 1
            except Exception as e:
                if verbose:
                    tqdm.write(f"  Phase 1b error for {address}: {e}")

            time.sleep(sleep_time)

    # Phase 2: Hours 24-192 (days 1-8) at 1-hour intervals
    if now > end_24h:
        phase2_end = min(end_8d, now)
        phases_attempted += 1

        try:
            df3 = safe_api_call(api, address, end_24h, phase2_end, "1H")
            if df3 is not None and len(df3) > 0:
                df3['interval'] = '1H'
                all_dfs.append(df3)
                phases_succeeded += 1
        except Exception as e:
            if verbose:
                tqdm.write(f"  Phase 2 error for {address}: {e}")

        time.sleep(sleep_time)

    # Phase 3: After day 8 at 12-hour intervals
    if now > end_8d:
        phases_attempted += 1
        try:
            df4 = safe_api_call(api, address, end_8d, now, "12H")
            if df4 is not None and len(df4) > 0:
                df4['interval'] = '12H'
                all_dfs.append(df4)
                phases_succeeded += 1
        except Exception as e:
            if verbose:
                tqdm.write(f"  Phase 3 error for {address}: {e}")

    # Combine all dataframes
    status = f"{phases_succeeded}/{phases_attempted} phases"

    if not all_dfs:
        return None, status

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Sort by timestamp and remove duplicates (in case of overlap at boundaries)
    combined_df = combined_df.sort_values('ts').drop_duplicates(subset=['ts'], keep='first')

    return combined_df, status


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch OHLCV history for addresses in pump coin_search CSVs')
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
    parser.add_argument('--single-interval', type=str, default=None,
                       choices=['1m', '3m', '5m', '15m', '30m', '1H', '2H', '4H', '6H', '8H', '12H', '1D', '3D', '1W', '1M'],
                       help='Use single interval instead of multi-resolution (default: multi-resolution)')
    parser.add_argument('--reset', action='store_true',
                       help='Reset progress tracking and start fresh')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Retry previously failed addresses')
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
    if args.single_interval:
        print(f"OHLCV interval: {args.single_interval} (single interval mode)")
    else:
        print("OHLCV mode: Multi-resolution (1m for 24h, 1H for days 1-8, 12H after)")

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

    # Load or reset progress tracking
    if args.reset:
        progress = {"completed": [], "failed": [], "skipped": []}
        print("Progress tracking reset.")
    else:
        progress = load_progress()
        prev_completed = len(progress.get("completed", []))
        prev_failed = len(progress.get("failed", []))
        if prev_completed or prev_failed:
            print(f"Resuming: {prev_completed} completed, {prev_failed} failed from previous run")

    # Handle --retry-failed: remove failed from tracking so they'll be retried
    if args.retry_failed:
        retry_count = len(progress.get("failed", []))
        progress["failed"] = []
        print(f"Will retry {retry_count} previously failed addresses")

    # Build set of addresses to skip (already in progress tracking)
    skip_set = set(progress.get("completed", []))
    if not args.retry_failed:
        skip_set.update(progress.get("failed", []))
    skip_set.update(progress.get("skipped", []))

    print(f"Starting to fetch OHLCV history for each address...")
    print()

    # Track successes, failures, skipped, and already processed
    successful = []
    failed = []
    skipped = []
    already_processed = []
    save_interval = 10  # Save progress every N addresses

    # Process each address
    pbar = tqdm(addresses, desc="Processing addresses", dynamic_ncols=True)
    processed_count = 0

    for address in pbar:
        try:
            # Check if OHLCV history file already exists
            output_file = f"data/OHLCV_history_{address}.csv"
            if Path(output_file).exists():
                already_processed.append(address)
                continue

            # Check if already processed in progress tracking
            if address in skip_set:
                already_processed.append(address)
                continue

            # Update progress bar with current address
            pbar.set_postfix_str(f"{address[:12]}...")

            # Fetch security details FIRST if requested (so creation time can be used)
            security_status = ""
            if args.fetch_security:
                try:
                    security = api.get_token_security_details(address)
                    security_status = "sec:ok" if security else "sec:none"
                except Exception as e:
                    security_status = "sec:err"

            # Now get creation time (will use security_details.csv if available)
            creation_time = api._get_token_creation_time(address)

            # For multi-resolution mode, we require creation time
            if args.single_interval is None:
                # Multi-resolution mode
                if creation_time is None:
                    skipped.append(address)
                    progress.setdefault("skipped", []).append(address)
                    if args.verbose:
                        tqdm.write(f"SKIP {address} (multi-res requires creation time)")
                    time.sleep(0.5)
                    continue

                # Fetch multi-resolution OHLCV data
                df, phase_status = fetch_multi_resolution_ohlcv(
                    api, address, creation_time, sleep_time=args.sleep_time, verbose=args.verbose
                )
            else:
                # Single interval mode (original behavior)
                phase_status = ""
                if creation_time is not None:
                    start_ts = creation_time
                elif args.skip_missing:
                    skipped.append(address)
                    progress.setdefault("skipped", []).append(address)
                    if args.verbose:
                        tqdm.write(f"SKIP {address} (no creation time)")
                    time.sleep(0.5)
                    continue
                else:
                    start_ts = default_start_ts

                df = api.get_ohlcv_history(address, start_ts=start_ts, interval=args.single_interval)

            if df is not None and len(df) > 0:
                # Save to CSV
                df.to_csv(output_file, mode='w', header=True, index=False)
                successful.append(address)
                progress.setdefault("completed", []).append(address)
                if args.verbose:
                    intervals_used = df['interval'].unique().tolist() if 'interval' in df.columns else [args.single_interval]
                    tqdm.write(f"OK   {address} | {len(df):>4} records | {phase_status} | intervals: {intervals_used} {security_status}")
            else:
                failed.append(address)
                progress.setdefault("failed", []).append(address)
                if args.verbose:
                    tqdm.write(f"FAIL {address} | no data | {phase_status} {security_status}")

            processed_count += 1

            # Save progress periodically
            if processed_count % save_interval == 0:
                save_progress(progress)

        except Exception as e:
            failed.append(address)
            progress.setdefault("failed", []).append(address)
            if args.verbose:
                tqdm.write(f"ERR  {address} | {e}")

        # Sleep to avoid rate limits (only for single-interval mode;
        # multi-resolution handles its own sleep)
        if args.single_interval is not None:
            time.sleep(args.sleep_time)

    # Final progress save
    save_progress(progress)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    if args.single_interval:
        print(f"Mode: Single interval ({args.single_interval})")
    else:
        print("Mode: Multi-resolution (1m/1H/12H)")
    print(f"CSV files processed: {len(csv_files)}")
    print(f"Total unique addresses: {len(addresses)}")
    print(f"Already processed (skipped): {len(already_processed)}")
    print(f"Newly successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Skipped (no creation time): {len(skipped)}")
    print(f"\nProgress tracking:")
    print(f"  Total completed (all runs): {len(progress.get('completed', []))}")
    print(f"  Total failed (all runs): {len(progress.get('failed', []))}")
    print(f"  Progress file: {PROGRESS_FILE}")
    if args.fetch_security:
        print(f"\nSecurity details saved to: data/security_details.csv")
    print(f"\nOHLCV files saved to: data/OHLCV_history_<address>.csv")
    print(f"\nTip: Use --retry-failed to retry failed addresses, --reset to start fresh")

    if failed:
        print(f"\nFailed addresses:")
        for addr in failed[:20]:  # Limit to first 20
            print(f"  - {addr}")
        if len(failed) > 20:
            print(f"  ... and {len(failed) - 20} more")

    if skipped:
        print(f"\nSkipped addresses (no creation time):")
        for addr in skipped[:20]:  # Limit to first 20
            print(f"  - {addr}")
        if len(skipped) > 20:
            print(f"  ... and {len(skipped) - 20} more")

    print("\nDone!")

if __name__ == "__main__":
    main()
