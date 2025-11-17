import requests
import sys
from pathlib import Path
import json
from datetime import datetime
import pandas as pd
from typing import Optional
from urllib.parse import urlencode
import time

with open( 'api_key', 'r' ) as f:
    api_key = f.readline().strip()

if not api_key:
    print( f'Error reading API key' )
    sys.exit(1)

class QuotaExceededException(Exception):
    """Raised when API quota is exceeded"""
    pass

def get_metadata( token_address: str ):
    url = f"https://solana-gateway.moralis.io/token/mainnet/{token_address}/metadata"

    headers = {
      "Accept": "application/json",
      "X-API-Key": api_key
    }

    response = requests.request("GET", url, headers=headers)

    print(response.text)

def get_price_by_token(
    token_address: str,
    start_date: datetime,
    end_date: Optional[datetime] = None,
    timeframe: str = "1h",
    currency: str = "usd",
    delay_between_requests: float = 1.0
) -> dict:
    """
    Get historical prices for all pairs where the token is the base token.

    Args:
        token_address: The token address to get price history for
        start_date: Start date for the price history
        end_date: End date for the price history (defaults to now)
        timeframe: Timeframe for OHLCV data (e.g., "1h", "1d")
        currency: Currency for price data (default "usd")
        delay_between_requests: Delay in seconds between pair requests (default 1.0)

    Returns:
        dict mapping pair_address to DataFrame with OHLCV data
    """
    # Check if pairs file already exists
    data_dir = Path('data')
    pairs_file = data_dir / f'pairs_{token_address}.csv'

    if pairs_file.exists():
        print(f"Reading existing pairs file: {pairs_file}")
        pairs_df = pd.read_csv(pairs_file)
    else:
        print(f"Pairs file not found, fetching from API...")
        pairs_df = get_pairs(token_address)

    if pairs_df.empty:
        print("No pairs found")
        return {}

    # Filter to only pairs where this token is the base token
    base_pairs = pairs_df[pairs_df['baseToken'] == token_address]
    print(f"Found {len(base_pairs)} pairs where {token_address} is the base token")

    if base_pairs.empty:
        print("No pairs found where token is the base token")
        return {}

    # Get list of pair addresses
    pair_addresses = base_pairs['pairAddress'].tolist()

    # Prepare consolidated CSV file
    consolidated_file = data_dir / f'price_history_{token_address}.csv'

    # Load existing data if file exists
    if consolidated_file.exists():
        print(f"Loading existing consolidated file: {consolidated_file}")
        consolidated_df = pd.read_csv(consolidated_file)
        # Convert timestamp back to datetime
        if 'timestamp' in consolidated_df.columns:
            consolidated_df['timestamp'] = pd.to_datetime(consolidated_df['timestamp'])
    else:
        consolidated_df = pd.DataFrame()

    # Fetch historical prices for each pair
    results = {}
    for i, pair_address in enumerate(pair_addresses, 1):
        print(f"\n[{i}/{len(pair_addresses)}] Fetching prices for pair: {pair_address}")
        try:
            price_df = get_historical_prices(
                pair_address=pair_address,
                start_date=start_date,
                end_date=end_date,
                timeframe=timeframe,
                currency=currency
            )
            if not price_df.empty:
                results[pair_address] = price_df

                # Add pair_address column to the dataframe
                price_df = price_df.copy()
                price_df['pair_address'] = pair_address
                price_df['token_address'] = token_address

                # Append to consolidated dataframe
                if consolidated_df.empty:
                    consolidated_df = price_df
                else:
                    # Remove duplicates based on timestamp and pair_address before concatenating
                    existing_keys = set(zip(consolidated_df['timestamp'], consolidated_df['pair_address']))
                    new_data = price_df[~price_df.apply(lambda x: (x['timestamp'], x['pair_address']) in existing_keys, axis=1)]
                    if not new_data.empty:
                        consolidated_df = pd.concat([consolidated_df, new_data], ignore_index=True)

                # Save consolidated file after each pair
                consolidated_df = consolidated_df.sort_values(['pair_address', 'timestamp'])
                consolidated_df.to_csv(consolidated_file, index=False)
                print(f"Updated consolidated file: {consolidated_file} (total records: {len(consolidated_df)})")

        except QuotaExceededException:
            print(f"\nQuota exceeded. Stopping data collection.")
            print(f"Successfully saved data for {len(results)} pairs before quota limit.")
            raise
        except Exception as e:
            print(f"Error fetching prices for {pair_address}: {e}")
            continue

        # Add delay between requests to avoid rate limiting
        if i < len(pair_addresses):
            time.sleep(delay_between_requests)

    print(f"\nSuccessfully fetched price history for {len(results)}/{len(pair_addresses)} pairs")
    print(f"Consolidated data saved to: {consolidated_file}")
    return results


def get_pairs(token_address: str, max_retries: int = 5, initial_backoff: float = 30.0) -> pd.DataFrame:
    """
    Get all trading pairs for a token from Moralis API.

    Args:
        token_address: The token address to get pairs for
        max_retries: Maximum number of retries for rate limit errors (default 5)
        initial_backoff: Initial backoff delay in seconds (default 30.0)

    Returns:
        pandas DataFrame with pair data
    """
    base_url = f"https://solana-gateway.moralis.io/token/mainnet/{token_address}/pairs"

    headers = {
        "Accept": "application/json",
        "X-API-Key": api_key
    }

    all_pairs = []
    cursor = None
    page = 1

    while True:
        # Build query parameters
        params = {
            "limit": 25
        }

        if cursor:
            params["cursor"] = cursor

        # Retry logic with exponential backoff for rate limiting
        retry_count = 0
        backoff_delay = initial_backoff

        while retry_count <= max_retries:
            try:
                print(f"Fetching page {page}..." + (f" (retry {retry_count})" if retry_count > 0 else ""))
                response = requests.get(base_url, headers=headers, params=params, timeout=30)

                # Check for quota exceeded error (401 with validation service blocked)
                if response.status_code == 401:
                    try:
                        error_data = response.json()
                        if "Validation service blocked" in error_data.get("message", ""):
                            print(f"\n{'='*80}")
                            print("ERROR: API quota exceeded!")
                            print(error_data.get("message", ""))
                            print(f"{'='*80}\n")
                            raise QuotaExceededException(error_data.get("message", "Quota exceeded"))
                    except (json.JSONDecodeError, QuotaExceededException):
                        if isinstance(sys.exc_info()[1], QuotaExceededException):
                            raise
                        # If JSON decode fails, raise for status anyway
                        response.raise_for_status()

                # Check for rate limit error
                if response.status_code == 429:
                    if retry_count < max_retries:
                        print(f"Rate limit hit. Waiting {backoff_delay:.1f}s before retry...")
                        time.sleep(backoff_delay)
                        retry_count += 1
                        backoff_delay *= 2  # Exponential backoff
                        continue
                    else:
                        print(f"Max retries ({max_retries}) reached for rate limit. Skipping.")
                        return pd.DataFrame() if not all_pairs else None  # Signal to process what we have

                response.raise_for_status()

                data = response.json()

                # Extract pairs
                if "pairs" in data and data["pairs"]:
                    all_pairs.extend(data["pairs"])
                    print(f"Retrieved {len(data['pairs'])} pairs")
                else:
                    print("No more pairs")
                    break

                # Check for pagination cursor
                if "cursor" in data and data["cursor"]:
                    cursor = data["cursor"]
                    page += 1
                else:
                    # No more pages
                    break

                # Break out of retry loop on success
                break

            except requests.exceptions.RequestException as e:
                if retry_count < max_retries and hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                    print(f"Rate limit error. Waiting {backoff_delay:.1f}s before retry...")
                    time.sleep(backoff_delay)
                    retry_count += 1
                    backoff_delay *= 2
                    continue
                else:
                    print(f"Error fetching data: {e}")
                    if hasattr(e, 'response') and e.response is not None:
                        print(f"Response: {e.response.text}")
                    # Return what we have so far
                    break
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                break

        # If we exhausted retries, break the main pagination loop
        if retry_count > max_retries:
            break

    # Convert to DataFrame
    if all_pairs:
        # Flatten the nested 'pair' array structure
        flattened_pairs = []
        for pair_data in all_pairs:
            flat_pair = {
                'exchangeAddress': pair_data.get('exchangeAddress'),
                'exchangeName': pair_data.get('exchangeName'),
                'exchangeLogo': pair_data.get('exchangeLogo'),
                'pairAddress': pair_data.get('pairAddress'),
                'pairLabel': pair_data.get('pairLabel'),
                'usdPrice': pair_data.get('usdPrice'),
                'usdPrice24hrPercentChange': pair_data.get('usdPrice24hrPercentChange'),
                'usdPrice24hrUsdChange': pair_data.get('usdPrice24hrUsdChange'),
                'volume24hrNative': pair_data.get('volume24hrNative'),
                'volume24hrUsd': pair_data.get('volume24hrUsd'),
                'liquidityUsd': pair_data.get('liquidityUsd'),
                'baseToken': pair_data.get('baseToken'),
                'quoteToken': pair_data.get('quoteToken'),
                'inactivePair': pair_data.get('inactivePair')
            }

            # Flatten the pair array (token0 and token1)
            if 'pair' in pair_data and pair_data['pair']:
                for i, token in enumerate(pair_data['pair']):
                    flat_pair[f'token{i}_address'] = token.get('tokenAddress')
                    flat_pair[f'token{i}_name'] = token.get('tokenName')
                    flat_pair[f'token{i}_symbol'] = token.get('tokenSymbol')
                    flat_pair[f'token{i}_logo'] = token.get('tokenLogo')
                    flat_pair[f'token{i}_decimals'] = token.get('tokenDecimals')
                    flat_pair[f'token{i}_pairTokenType'] = token.get('pairTokenType')
                    flat_pair[f'token{i}_liquidityUsd'] = token.get('liquidityUsd')

            flattened_pairs.append(flat_pair)

        df = pd.DataFrame(flattened_pairs)
        print(f"Total pairs retrieved: {len(df)}")

        # Save to CSV
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        output_file = data_dir / f'pairs_{token_address}.csv'
        df.to_csv(output_file, index=False)
        print(f"Saved pairs data to {output_file}")

        return df
    else:
        print("No pairs retrieved")
        return pd.DataFrame()


def get_historical_prices(
    pair_address: str,
    start_date: datetime,
    end_date: Optional[datetime] = None,
    timeframe: str = "1h",
    currency: str = "usd",
    max_retries: int = 5,
    initial_backoff: float = 30.0
) -> pd.DataFrame:
    """
    Get the entire price history for a token pair from Moralis API.

    Args:
        pair_address: The token pair address
        start_date: Start date for the price history
        end_date: End date for the price history (defaults to now)
        timeframe: Timeframe for OHLCV data (e.g., "1h", "1d")
        currency: Currency for price data (default "usd")
        max_retries: Maximum number of retries for rate limit errors (default 5)
        initial_backoff: Initial backoff delay in seconds (default 30.0)

    Returns:
        pandas DataFrame with OHLCV data
    """
    if end_date is None:
        end_date = datetime.now()

    # Format dates for URL
    from_date = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_date = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    base_url = f"https://solana-gateway.moralis.io/token/mainnet/pairs/{pair_address}/ohlcv"

    headers = {
        "Accept": "application/json",
        "X-API-Key": api_key
    }

    all_results = []
    cursor = None
    page = 1

    while True:
        # Build query parameters
        params = {
            "fromDate": from_date,
            "toDate": to_date,
            "timeframe": timeframe,
            "currency": currency,
            "limit": 100  # Max results per page
        }

        if cursor:
            params["cursor"] = cursor

        # Retry logic with exponential backoff for rate limiting
        retry_count = 0
        backoff_delay = initial_backoff

        while retry_count <= max_retries:
            try:
                print(f"Fetching page {page}..." + (f" (retry {retry_count})" if retry_count > 0 else ""))
                response = requests.get(base_url, headers=headers, params=params, timeout=30)

                # Check for quota exceeded error (401 with validation service blocked)
                if response.status_code == 401:
                    try:
                        error_data = response.json()
                        if "Validation service blocked" in error_data.get("message", ""):
                            print(f"\n{'='*80}")
                            print("ERROR: API quota exceeded!")
                            print(error_data.get("message", ""))
                            print(f"{'='*80}\n")
                            raise QuotaExceededException(error_data.get("message", "Quota exceeded"))
                    except (json.JSONDecodeError, QuotaExceededException):
                        if isinstance(sys.exc_info()[1], QuotaExceededException):
                            raise
                        # If JSON decode fails, raise for status anyway
                        response.raise_for_status()

                # Check for rate limit error
                if response.status_code == 429:
                    if retry_count < max_retries:
                        print(f"Rate limit hit. Waiting {backoff_delay:.1f}s before retry...")
                        time.sleep(backoff_delay)
                        retry_count += 1
                        backoff_delay *= 2  # Exponential backoff
                        continue
                    else:
                        print(f"Max retries ({max_retries}) reached for rate limit. Skipping.")
                        return pd.DataFrame() if not all_results else None  # Signal to process what we have

                response.raise_for_status()

                data = response.json()

                # Extract results
                if "result" in data and data["result"]:
                    all_results.extend(data["result"])
                    print(f"Retrieved {len(data['result'])} records")
                else:
                    print("No more results")
                    break

                # Check for pagination cursor
                if "cursor" in data and data["cursor"]:
                    cursor = data["cursor"]
                    page += 1
                else:
                    # No more pages
                    break

                # Break out of retry loop on success
                break

            except requests.exceptions.RequestException as e:
                if retry_count < max_retries and hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                    print(f"Rate limit error. Waiting {backoff_delay:.1f}s before retry...")
                    time.sleep(backoff_delay)
                    retry_count += 1
                    backoff_delay *= 2
                    continue
                else:
                    print(f"Error fetching data: {e}")
                    if hasattr(e, 'response') and e.response is not None:
                        print(f"Response: {e.response.text}")
                    # Return what we have so far
                    break
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                break

        # If we exhausted retries, break the main pagination loop
        if retry_count > max_retries:
            break

    # Convert to DataFrame
    if all_results:
        df = pd.DataFrame(all_results)
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Sort by timestamp
        df = df.sort_values('timestamp')
        print(f"Total records retrieved: {len(df)}")

        # Save to CSV
        data_dir = Path('data')
        data_dir.mkdir(exist_ok=True)
        output_file = data_dir / f'pair_history_{pair_address}.csv'
        df.to_csv(output_file, index=False)
        print(f"Saved price history to {output_file}")

        return df
    else:
        print("No data retrieved")
        return pd.DataFrame()

