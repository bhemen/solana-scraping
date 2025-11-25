#!/usr/bin/env python3
"""
Fetch trending tokens from Birdeye using pagination.
Uses the BirdeyeAPI get_token_trending function.
"""

import sys
from pathlib import Path
from datetime import datetime
from birdeye_utils import BirdeyeAPI

# Disable tqdm if running from cron / ipython
# https://github.com/tqdm/tqdm/issues/506
try:
    ipy_str = str(type(get_ipython()))
    if 'zmqshell' in ipy_str:
        from tqdm import tqdm_notebook as tqdm
    if 'terminal' in ipy_str:
        from tqdm import tqdm
except:
    if sys.stderr.isatty():
        from tqdm import tqdm
    else:
        def tqdm(iterable, **kwargs):
            return iterable

# Configuration
LIMIT_PER_REQUEST = 20
NUM_TOKENS = 10000

# Output file with today's date
today = datetime.today().strftime('%Y-%m-%d')
outfile = f"data/top_tokens-{today}.csv"

# Initialize API client
api = BirdeyeAPI(verbose=False)

print(f"Fetching {NUM_TOKENS} trending tokens...")
print(f"Output file: {outfile}")

# Track all tokens
all_tokens = []
offset = 0

# Fetch tokens with pagination
for i in tqdm(range(NUM_TOKENS // LIMIT_PER_REQUEST), desc="Fetching trending tokens"):
    # Get trending tokens for this page
    df = api.get_token_trending(
        sort_by="rank",
        sort_type="asc",
        offset=offset,
        limit=LIMIT_PER_REQUEST
    )

    if df is None or len(df) == 0:
        print(f"\nNo more tokens available at offset {offset}")
        break

    # Add to collection
    all_tokens.append(df)

    # Increment offset for next page
    offset += LIMIT_PER_REQUEST

# Combine all tokens into one DataFrame
if all_tokens:
    import pandas as pd
    combined_df = pd.concat(all_tokens, ignore_index=True)

    # Add timestamp
    combined_df['ts'] = int(datetime.now().timestamp())

    # Save to CSV
    combined_df.to_csv(outfile, index=False)

    print(f"\n{'='*60}")
    print(f"SUCCESS")
    print(f"{'='*60}")
    print(f"Fetched {len(combined_df)} tokens")
    print(f"Saved to: {outfile}")
    print(f"\nTop 5 tokens by rank:")
    print(combined_df[['rank', 'symbol', 'name', 'price', 'volume24hUSD']].head())
else:
    print("No tokens fetched")
