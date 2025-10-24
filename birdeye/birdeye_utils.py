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


class BirdeyeAPI:
    """Class for interacting with the Birdeye API."""

    BASE_URL = "https://public-api.birdeye.so"
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_WAIT_TIME = 5

    def __init__(self, api_key: Optional[str] = None, max_retries: int = DEFAULT_MAX_RETRIES):
        """
        Initialize the Birdeye API client.

        Args:
            api_key: Birdeye API key. If None, will try to load from 'api_key' file.
            max_retries: Maximum number of retry attempts for failed requests.
        """
        self.api_key = api_key or self._load_api_key()
        self.max_retries = max_retries
        self.error_file = "data/birdeye_errors.csv"

        # Ensure data directory exists
        Path("data").mkdir(exist_ok=True)

    def _load_api_key(self) -> Optional[str]:
        """Load API key from file."""
        dir_path = os.path.dirname(os.path.realpath(__file__))
        try:
            with open(f'{dir_path}/api_key', 'r') as file:
                return file.read().rstrip()
        except Exception as e:
            print(f"Warning: Could not load API key from file: {e}")
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
        Get the creation time for a token from metadata.

        Args:
            address: Token address

        Returns:
            Unix timestamp of token creation, or None if not available
        """
        metadata = self.get_token_metadata(address)

        if metadata is None:
            print(f"Warning: Could not retrieve metadata for {address}")
            return None

        # Try to extract creation_time from meme_info
        try:
            if 'meme_info' in metadata and 'creation_time' in metadata['meme_info']:
                creation_time = metadata['meme_info']['creation_time']
                # Handle both int and string timestamps
                return int(creation_time) if creation_time else None
        except (KeyError, TypeError, ValueError) as e:
            print(f"Warning: Could not extract creation_time from metadata for {address}: {e}")

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
                print(f"Warning: Could not determine start time for {address}, cannot fetch price history")
                return None
            print(f"Using token creation time as start: {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')}")

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

    # Get token metadata
    print("\nFetching token metadata...")
    metadata = api.get_token_metadata(example_address)
    if metadata is not None:
        print("Token metadata:")
        for key, value in list(metadata.items())[:5]:  # Show first 5 fields
            print(f"  {key}: {value}")
        api.save_token_metadata_to_csv(example_address)
