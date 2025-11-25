"""
Utility functions for interacting with the Birdeye API.

This module provides functions to query the Birdeye API for:
- Historical price data
- Token metadata (meme tokens)

Includes automatic retry logic with exponential backoff for rate limiting.
"""

import requests
import os
import csv
import pandas as pd
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv


class BirdeyeAPI:
    """Class for interacting with the Birdeye API."""

    BASE_URL = "https://public-api.birdeye.so"
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_WAIT_TIME = 5

    def __init__(self, api_key: Optional[str] = None, max_retries: int = DEFAULT_MAX_RETRIES, verbose: bool = True):
        """
        Initialize the Birdeye API client.

        Args:
            api_key: Birdeye API key. If None, will try to load from 'api_key' file.
            max_retries: Maximum number of retry attempts for failed requests.
            verbose: If True, print status messages. If False, suppress output.
        """
        self.api_key = api_key or self._load_api_key()
        self.max_retries = max_retries
        self.verbose = verbose
        self.error_file = "data/birdeye_errors.csv"

        # Ensure data directory exists
        Path("data").mkdir(exist_ok=True)

    def _print(self, message: str):
        """Print message only if verbose mode is enabled."""
        if self.verbose:
            print(message)

    def _load_api_key(self) -> Optional[str]:
        """Load API key from .env.api file using dotenv."""
        dir_path = os.path.dirname(os.path.realpath(__file__))
        env_file = os.path.join(dir_path, '.env.api')

        try:
            # Load environment variables from .env.api file
            load_dotenv(env_file)
            api_key = os.getenv('BIRDEYE_API_KEY')

            if api_key:
                return api_key
            else:
                print(f"Warning: BIRDEYE_API_KEY not found in {env_file}")
                return None
        except Exception as e:
            print(f"Warning: Could not load API key from {env_file}: {e}")
            return None

    def _get_headers(self, chain: str = "solana") -> Dict[str, str]:
        """Get request headers for API calls."""
        return {
            "accept": "application/json",
            "x-chain": chain,
            "X-API-KEY": self.api_key
        }

    def _log_error(self, address: str, error: str, url: Optional[str] = None):
        """Log an error to the error file."""
        try:
            with open(self.error_file, "a") as f:
                timestamp = datetime.now().isoformat()
                if url:
                    f.write(f"{timestamp},{address},{error},{url}\n")
                else:
                    f.write(f"{timestamp},{address},{error}\n")
        except Exception as e:
            print(f"Warning: Could not log error: {e}")

    def _make_request(
        self,
        url: str,
        identifier: str,
        retry: int = 0,
        wait: int = DEFAULT_WAIT_TIME
    ) -> Optional[Dict[str, Any]]:
        """
        Make an API request with exponential backoff retry logic.

        Args:
            url: The full URL to request
            identifier: An identifier (e.g., token address) for error logging
            retry: Current retry attempt number
            wait: Wait time in seconds before retry

        Returns:
            JSON response as dict, or None if request failed
        """
        try:
            response = requests.get(url, headers=self._get_headers())
        except Exception as e:
            self._log_error(identifier, str(e), url)
            if retry < self.max_retries:
                time.sleep(wait)
                return self._make_request(url, identifier, retry + 1, wait * 2)
            return None

        if response.status_code == 200:
            try:
                return response.json()
            except Exception as e:
                self._log_error(identifier, f"JSON decode error: {e}", url)
                if retry < self.max_retries:
                    time.sleep(wait)
                    return self._make_request(url, identifier, retry + 1, wait * 2)
                return None
        elif response.status_code == 401:
            # Permission denied - don't retry, this won't succeed
            self._log_error(identifier, f"HTTP 401: {response.text}", url)
            return None
        elif response.status_code == 429:
            # Rate limit hit - use longer backoff
            self._log_error(identifier, f"Rate limit (429)", url)
            if retry < self.max_retries:
                wait_time = wait * 3  # Extra long wait for rate limits
                print(f"Rate limit hit. Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                return self._make_request(url, identifier, retry + 1, wait_time * 2)
            return None
        else:
            self._log_error(identifier, f"HTTP {response.status_code}: {response.text}", url)
            if retry < self.max_retries:
                time.sleep(wait)
                return self._make_request(url, identifier, retry + 1, wait * 2)
            return None

    def _get_token_creation_time(self, address: str) -> Optional[int]:
        """
        Get the creation time for a token from security_details.csv, metadata, or token_creation_info.csv.

        Checks sources in this order:
        1. security_details.csv (if exists)
        2. metadata API
        3. token_creation_info.csv

        Args:
            address: Token address

        Returns:
            Unix timestamp of token creation, or None if not available
        """
        # First, check security_details.csv (fastest, no API call)
        try:
            security_csv = "data/security_details.csv"
            if Path(security_csv).exists():
                df = pd.read_csv(security_csv)
                existing = df[df['address'] == address]
                if len(existing) > 0:
                    creation_time = existing.iloc[0].get('creationTime')
                    if pd.notna(creation_time) and creation_time:
                        creation_time = int(creation_time)
                        self._print(f"Using creation time from security_details.csv: {datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')}")
                        return creation_time
        except Exception as e:
            self._print(f"Warning: Could not get creation time from security_details.csv for {address}: {e}")

        # Second, try to get from metadata API
        metadata = self.get_token_metadata(address)

        if metadata is not None:
            # Try to extract creation_time from meme_info
            try:
                if 'meme_info' in metadata and 'creation_time' in metadata['meme_info']:
                    creation_time = metadata['meme_info']['creation_time']
                    # Handle both int and string timestamps
                    return int(creation_time) if creation_time else None
            except (KeyError, TypeError, ValueError) as e:
                self._print(f"Warning: Could not extract creation_time from metadata for {address}: {e}")

        # Fall back to token_creation_info.csv
        try:
            creation_info_file = "data/token_creation_info.csv"
            if Path(creation_info_file).exists():
                df = pd.read_csv(creation_info_file)
                df_filtered = df[df.tokenAddress == address]
                if len(df_filtered) > 0:
                    block_time = int(df_filtered.blockUnixTime.values[0])
                    self._print(f"Using creation time from token_creation_info.csv: {datetime.fromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S')}")
                    return block_time
        except Exception as e:
            self._print(f"Warning: Could not get creation time from token_creation_info.csv for {address}: {e}")

        self._print(f"Warning: Could not determine creation time for {address} from any source")
        return None

    def get_price_history(
        self,
        address: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        period: str = "1D",
        address_type: str = "token"
    ) -> Optional[pd.DataFrame]:
        """
        Get historical price data for a token.

        API Documentation: https://docs.birdeye.so/reference/get-defi-history_price

        Args:
            address: Token address
            start_ts: Start timestamp (Unix timestamp in seconds).
                     If None, uses token creation time from metadata.
            end_ts: End timestamp (Unix timestamp in seconds).
                   If None, uses current time.
            period: Time period granularity (e.g., "1D", "1H", "5m")
            address_type: Type of address (default: "token")

        Returns:
            DataFrame with columns: ts, price, date (or None if request failed)
        """
        # Handle optional start_ts
        if start_ts is None:
            start_ts = self._get_token_creation_time(address)
            if start_ts is None:
                self._print(f"Warning: Could not determine start time for {address}, cannot fetch price history")
                return None
            self._print(f"Using token creation time as start: {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')}")

        # Handle optional end_ts
        if end_ts is None:
            end_ts = int(datetime.now().timestamp())

        url = (
            f"{self.BASE_URL}/defi/history_price"
            f"?address={address}"
            f"&address_type={address_type}"
            f"&type={period}"
            f"&time_from={int(start_ts)}"
            f"&time_to={int(end_ts)}"
        )

        json_response = self._make_request(url, address)

        if json_response is None:
            return None

        # Extract price history from response
        if 'data' in json_response and 'items' in json_response['data']:
            df = pd.DataFrame(json_response['data']['items'])

            # Rename columns to standard format
            df.rename(columns={
                'unixTime': 'ts',
                'value': 'price'
            }, inplace=True)

            # Add date column if timestamp exists
            if 'ts' in df.columns:
                df['date'] = pd.to_datetime(df['ts'], unit='s').dt.date

            return df
        else:
            self._log_error(address, f"Unexpected response format: {json_response}", url)
            return None

    def get_token_metadata(self, address: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a meme token.

        API Documentation: https://docs.birdeye.so/reference/get-defi-v3-token-meme-detail-single

        Args:
            address: Token address (Solana)
            force_refresh: If True, fetch from API even if cached file exists

        Returns:
            Dictionary containing token metadata, or None if request failed.
            Metadata includes: price, volume, market cap, price changes,
            popularity score, listing timestamp, and token metadata.

        Note: Metadata is automatically cached to data/metadata_{address}.json
        """
        import json

        metadata_file = f"data/metadata_{address}.json"

        # Check if cached file exists and we're not forcing a refresh
        if not force_refresh and Path(metadata_file).exists():
            try:
                with open(metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load cached metadata from {metadata_file}: {e}")
                # Continue to fetch from API

        # Fetch from API
        url = f"{self.BASE_URL}/defi/v3/token/meme/detail/single?address={address}"

        json_response = self._make_request(url, address)

        if json_response is None:
            return None

        # Return the data portion of the response
        if 'data' in json_response:
            metadata = json_response['data']

            # Save to cache file
            try:
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
            except Exception as e:
                print(f"Warning: Could not save metadata to {metadata_file}: {e}")

            return metadata
        else:
            self._log_error(address, f"No 'data' field in response: {json_response}", url)
            return None

    def save_price_history_to_csv(
        self,
        address: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        output_file: Optional[str] = None,
        period: str = "1D"
    ) -> bool:
        """
        Fetch price history and save it to a CSV file.

        Args:
            address: Token address
            start_ts: Start timestamp (Unix timestamp in seconds).
                     If None, uses token creation time from metadata.
            end_ts: End timestamp (Unix timestamp in seconds).
                   If None, uses current time.
            output_file: Path to output CSV file (default: data/price_history_{address}.csv)
            period: Time period granularity (default: "1D")

        Returns:
            True if successful, False otherwise
        """
        if output_file is None:
            output_file = f"data/price_history_{address}.csv"

        df = self.get_price_history(address, start_ts, end_ts, period)

        if df is None or len(df) == 0:
            print(f"No price history data retrieved for {address}")
            return False

        # Append or create new file
        if Path(output_file).is_file():
            # Append without header
            df.to_csv(output_file, mode='a', header=False, index=False)
        else:
            # Create new file with header
            df.to_csv(output_file, mode='w', header=True, index=False)

        print(f"Saved {len(df)} price records for {address} to {output_file}")
        return True

    def save_token_metadata_to_csv(
        self,
        address: str,
        output_file: str = "data/token_metadata.csv"
    ) -> bool:
        """
        Fetch token metadata and save it to a CSV file.

        Args:
            address: Token address
            output_file: Path to output CSV file (default: data/token_metadata.csv)

        Returns:
            True if successful, False otherwise
        """
        metadata = self.get_token_metadata(address)

        if metadata is None:
            print(f"No metadata retrieved for {address}")
            return False

        # Flatten nested dictionaries if needed
        flattened_data = self._flatten_dict(metadata)

        # Append or create new file
        if Path(output_file).is_file():
            with open(output_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=flattened_data.keys())
                writer.writerow(flattened_data)
        else:
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=flattened_data.keys())
                writer.writeheader()
                writer.writerow(flattened_data)

        print(f"Saved metadata for {address} to {output_file}")
        return True

    def get_token_security_details(
        self,
        address: str,
        force_refresh: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get security details for a token.

        API Documentation: https://docs.birdeye.so/reference/get-defi-token_security

        Args:
            address: Token address
            force_refresh: If True, fetch from API even if cached in CSV

        Returns:
            Dictionary containing token security details, or None if request failed.
            Security details include: creator info, owner info, creation/mint info,
            holder distribution, metadata mutability, supply info, and token flags.

        Note: Results are automatically cached to data/security_details.csv
        """
        security_csv = "data/security_details.csv"

        # Check if we have cached data (unless force refresh)
        if not force_refresh and Path(security_csv).exists():
            try:
                df = pd.read_csv(security_csv)
                # Check if this address already exists
                existing = df[df['address'] == address]
                if len(existing) > 0:
                    self._print(f"Using cached security details for {address}")
                    # Convert row to dictionary
                    return existing.iloc[0].to_dict()
            except Exception as e:
                self._print(f"Warning: Could not read cached security details: {e}")

        # Fetch from API
        url = f"{self.BASE_URL}/defi/token_security?address={address}"

        json_response = self._make_request(url, address)

        if json_response is None:
            return None

        # Extract security data from response
        if 'data' in json_response:
            security_data = json_response['data']

            # Add the address to the data
            security_data['address'] = address

            # Flatten nested dictionaries if any
            flattened_data = self._flatten_dict(security_data)

            # Append to CSV
            try:
                if Path(security_csv).exists():
                    # Check if address already exists (in case of race condition)
                    df = pd.read_csv(security_csv)
                    if address not in df['address'].values:
                        # Append new row
                        new_df = pd.DataFrame([flattened_data])
                        new_df.to_csv(security_csv, mode='a', header=False, index=False)
                    else:
                        # Update existing row
                        df.loc[df['address'] == address] = pd.Series(flattened_data)
                        df.to_csv(security_csv, index=False)
                else:
                    # Create new file
                    new_df = pd.DataFrame([flattened_data])
                    new_df.to_csv(security_csv, index=False)

                self._print(f"Saved security details for {address} to {security_csv}")
            except Exception as e:
                self._print(f"Warning: Could not save security details to CSV: {e}")

            return security_data
        else:
            self._log_error(address, f"No 'data' field in response: {json_response}", url)
            return None

    def get_token_trending(
        self,
        sort_by: str = "rank",
        sort_type: str = "asc",
        offset: int = 0,
        limit: int = 50,
        ui_amount_mode: str = "scaled"
    ) -> Optional[pd.DataFrame]:
        """
        Get trending tokens from Birdeye.

        API Documentation: https://docs.birdeye.so/reference/get-defi-token_trending

        Args:
            sort_by: Field to sort by (default: "rank")
                    Options: "rank", "volume24hUSD", "price24hChangePercent", etc.
            sort_type: Sort direction - "asc" or "desc" (default: "asc")
            offset: Pagination offset (default: 0)
            limit: Number of tokens to return (default: 50, max typically 100)
            ui_amount_mode: Amount display mode - "scaled" or "raw" (default: "scaled")

        Returns:
            DataFrame with trending token data, or None if request failed.
            Columns include: address, name, symbol, price, volume24hUSD,
            price24hChangePercent, rank, liquidity, fdv, marketcap, etc.
        """
        url = (
            f"{self.BASE_URL}/defi/token_trending"
            f"?sort_by={sort_by}"
            f"&sort_type={sort_type}"
            f"&offset={offset}"
            f"&limit={limit}"
            f"&ui_amount_mode={ui_amount_mode}"
        )

        json_response = self._make_request(url, "trending_tokens")

        if json_response is None:
            return None

        # Extract tokens from response
        if 'data' in json_response and 'tokens' in json_response['data']:
            tokens = json_response['data']['tokens']

            if tokens:
                df = pd.DataFrame(tokens)
                self._print(f"Retrieved {len(df)} trending tokens")
                return df
            else:
                self._print("No trending tokens found")
                return None
        else:
            self._log_error("trending_tokens", f"Unexpected response format: {json_response}", url)
            return None

    def get_token_trade_history(
        self,
        token_address: str,
        sort_by: str = "block_unix_time",
        after_time: Optional[int] = None,
        before_time: Optional[int] = None,
        after_block_number: Optional[int] = None,
        before_block_number: Optional[int] = None,
        limit_per_request: int = 100,
        max_total_records: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        Get complete trading history for a token using pagination.

        API Documentation: https://docs.birdeye.so/reference/get-defi-v3-token-txs

        Args:
            token_address: Token address to query
            sort_by: Sort field - either "block_unix_time" or "block_number" (default: "block_unix_time")
            after_time: Start timestamp (Unix timestamp in seconds). Use with sort_by="block_unix_time"
            before_time: End timestamp (Unix timestamp in seconds). Use with sort_by="block_unix_time"
                        Note: Max 30-day range when using time-based filters
            after_block_number: Start block number. Use with sort_by="block_number"
            before_block_number: End block number. Use with sort_by="block_number"
                               Note: Max 500,000 block range when using block-based filters
            limit_per_request: Number of records to fetch per API call (default: 100)
            max_total_records: Maximum total records to fetch across all pages (default: None = unlimited)

        Returns:
            DataFrame with all trade records, or None if request failed

        Note:
            - Only one filter type allowed: either time range OR block number range
            - Time-based filters require sort_by="block_unix_time"
            - Block-based filters require sort_by="block_number"
            - Default behavior: Last 7 days if sort_by="block_unix_time";
              last 500,000 blocks if sort_by="block_number"
        """
        all_trades = []
        offset = 0
        total_fetched = 0

        self._print(f"Fetching trade history for {token_address} (sort_by: {sort_by})...")

        while True:
            # Build URL with base parameters
            url = (
                f"{self.BASE_URL}/defi/v3/token/txs"
                f"?address={token_address}"
                f"&sort_by={sort_by}"
                f"&offset={offset}"
                f"&limit={limit_per_request}"
            )

            # Add time-based filters if using block_unix_time sorting
            if sort_by == "block_unix_time":
                if after_time is not None:
                    url += f"&after_time={int(after_time)}"
                if before_time is not None:
                    url += f"&before_time={int(before_time)}"
            # Add block-based filters if using block_number sorting
            elif sort_by == "block_number":
                if after_block_number is not None:
                    url += f"&after_block_number={int(after_block_number)}"
                if before_block_number is not None:
                    url += f"&before_block_number={int(before_block_number)}"

            # Make the request
            json_response = self._make_request(url, token_address)

            if json_response is None:
                # If we already have some data, return it; otherwise return None
                if all_trades:
                    self._print(f"Request failed at offset {offset}, returning {total_fetched} records fetched so far")
                    break
                else:
                    self._print(f"Failed to fetch trade history for {token_address}")
                    return None

            # Extract trades from response
            if 'data' in json_response and 'items' in json_response['data']:
                trades = json_response['data']['items']

                if not trades or len(trades) == 0:
                    # No more data available
                    self._print(f"No more trades available. Total fetched: {total_fetched}")
                    break

                all_trades.extend(trades)
                total_fetched += len(trades)
                self._print(f"  Fetched {len(trades)} trades (offset: {offset}, total: {total_fetched})")

                # Check if we've reached the maximum requested records
                if max_total_records and total_fetched >= max_total_records:
                    self._print(f"Reached maximum requested records ({max_total_records})")
                    all_trades = all_trades[:max_total_records]
                    break

                # Check if we got fewer records than requested (last page)
                if len(trades) < limit_per_request:
                    self._print(f"Received fewer records than requested. Reached end of data.")
                    break

                # Increment offset for next page
                offset += limit_per_request

            else:
                # Unexpected response format
                self._log_error(token_address, f"Unexpected response format at offset {offset}: {json_response}", url)
                if all_trades:
                    self._print(f"Unexpected response at offset {offset}, returning {total_fetched} records fetched so far")
                    break
                else:
                    return None

        # Convert to DataFrame
        if all_trades:
            df = pd.DataFrame(all_trades)
            self._print(f"Successfully fetched {len(df)} total trade records for {token_address}")
            return df
        else:
            self._print(f"No trade records found for {token_address}")
            return None

    @staticmethod
    def _flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Flatten a nested dictionary.

        Args:
            d: Dictionary to flatten
            parent_key: Key prefix for nested keys
            sep: Separator between nested keys

        Returns:
            Flattened dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(BirdeyeAPI._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert lists to comma-separated strings
                items.append((new_key, ','.join(map(str, v))))
            else:
                items.append((new_key, v))
        return dict(items)


def batch_get_price_history(
    addresses: List[str],
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    period: str = "1D",
    sleep_between_requests: float = 1.0,
    api_key: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Batch fetch price history for multiple token addresses.

    Args:
        addresses: List of token addresses
        start_ts: Start timestamp (Unix timestamp in seconds).
                 If None, uses token creation time from metadata for each token.
        end_ts: End timestamp (Unix timestamp in seconds).
               If None, uses current time.
        period: Time period granularity (default: "1D")
        sleep_between_requests: Seconds to sleep between requests (default: 1.0)
        api_key: Optional API key (if not provided, loads from file)

    Returns:
        Dictionary mapping addresses to their price history DataFrames
    """
    api = BirdeyeAPI(api_key=api_key)
    results = {}

    for address in addresses:
        print(f"Fetching price history for {address}...")
        df = api.get_price_history(address, start_ts, end_ts, period)

        if df is not None:
            results[address] = df
            print(f"  Retrieved {len(df)} records")
        else:
            print(f"  Failed to retrieve data")

        time.sleep(sleep_between_requests)

    return results


def batch_get_token_metadata(
    addresses: List[str],
    sleep_between_requests: float = 1.0,
    api_key: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Batch fetch token metadata for multiple token addresses.

    Args:
        addresses: List of token addresses
        sleep_between_requests: Seconds to sleep between requests (default: 1.0)
        api_key: Optional API key (if not provided, loads from file)

    Returns:
        Dictionary mapping addresses to their metadata
    """
    api = BirdeyeAPI(api_key=api_key)
    results = {}

    for address in addresses:
        print(f"Fetching metadata for {address}...")
        metadata = api.get_token_metadata(address)

        if metadata is not None:
            results[address] = metadata
            print(f"  Retrieved metadata")
        else:
            print(f"  Failed to retrieve metadata")

        time.sleep(sleep_between_requests)

    return results


# Example usage
if __name__ == "__main__":
    # Initialize API client
    api = BirdeyeAPI()

    # Example token address
    example_address = "D7rcV8SPxbv94s3kJETkrfMrWqHFs6qrmtbiu6saaany"

    # Example 1: Get price history from token creation to now (using defaults)
    print("Example 1: Fetching price history from token creation to now...")
    price_df = api.get_price_history(example_address, period="1D")
    if price_df is not None:
        print(f"Retrieved {len(price_df)} price records")
        print(price_df.head())

    # Example 2: Get price history for a specific time range
    print("\nExample 2: Fetching price history for the last 30 days...")
    end_ts = int(datetime.now().timestamp())
    start_ts = end_ts - (30 * 24 * 60 * 60)  # 30 days ago
    price_df = api.get_price_history(example_address, start_ts, end_ts, period="1D")
    if price_df is not None:
        print(f"Retrieved {len(price_df)} price records")
        print(price_df.head())
        api.save_price_history_to_csv(example_address, start_ts, end_ts)

    # Example 3: Get token metadata
    print("\nExample 3: Fetching token metadata...")
    metadata = api.get_token_metadata(example_address)
    if metadata is not None:
        print("Token metadata:")
        for key, value in list(metadata.items())[:5]:  # Show first 5 fields
            print(f"  {key}: {value}")
        api.save_token_metadata_to_csv(example_address)

    # Example 4: Get complete trade history (last 7 days by default)
    print("\nExample 4: Fetching complete trade history...")
    trades_df = api.get_token_trade_history(example_address)
    if trades_df is not None:
        print(f"Retrieved {len(trades_df)} trade records")
        print(trades_df.head())
        # Save to CSV
        trades_df.to_csv(f"data/trades_{example_address}.csv", index=False)
        print(f"Saved to data/trades_{example_address}.csv")

    # Example 5: Get trade history for a specific time range (last 3 days)
    print("\nExample 5: Fetching trade history for the last 3 days...")
    end_ts = int(datetime.now().timestamp())
    start_ts = end_ts - (3 * 24 * 60 * 60)  # 3 days ago
    trades_df = api.get_token_trade_history(
        example_address,
        sort_by="block_unix_time",
        after_time=start_ts,
        before_time=end_ts,
        max_total_records=500  # Limit to 500 records for demo
    )
    if trades_df is not None:
        print(f"Retrieved {len(trades_df)} trade records")
        print(trades_df.head())

    # Example 6: Get token security details
    print("\nExample 6: Fetching token security details...")
    security_details = api.get_token_security_details(example_address)
    if security_details is not None:
        print("Token security details:")
        # Show some key security fields
        key_fields = ['creatorPercentage', 'ownerPercentage', 'top10HolderPercent',
                      'mutableMetadata', 'freezeable', 'totalSupply', 'jupStrictList']
        for field in key_fields:
            if field in security_details:
                print(f"  {field}: {security_details[field]}")

    # Example 7: Get security details again (should use cached data)
    print("\nExample 7: Fetching security details again (should use cache)...")
    security_details = api.get_token_security_details(example_address)
    if security_details is not None:
        print("Successfully retrieved from cache")

    # Example 8: Get trending tokens
    print("\nExample 8: Fetching top 10 trending tokens...")
    trending_df = api.get_token_trending(limit=10, sort_by="rank", sort_type="asc")
    if trending_df is not None:
        print(f"Retrieved {len(trending_df)} trending tokens")
        print("\nTop 5 trending tokens:")
        print(trending_df[['rank', 'symbol', 'name', 'price', 'volume24hUSD', 'price24hChangePercent']].head())
        # Save to CSV
        trending_df.to_csv("data/trending_tokens.csv", index=False)
        print("Saved to data/trending_tokens.csv")
