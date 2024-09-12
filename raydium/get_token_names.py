"""
Reads transactions from 'data/raydiumTxsProcessed.csv'
Gets a list of all tokens traded
Checks token_file to see which token symbols are known
For tokens without known symbols, it calls get_symbol (from token_utils.py) to add the symbol
Rewrites data to token_file
"""

import pandas as pd
import ast
import os
from token_utils import get_symbol

json_cols = ['tokenBalances'] #These columns are JSON strings, so we will need to tell pandas to parse the JSON using ast.literal_eval (note, json.loads does not work well because they have single-quoted field names which is invalid JSON)
df = pd.read_csv( "data/raydiumTxsProcessed.csv",  converters={ c : ast.literal_eval for c in json_cols } )

token_list = set()

for bal in df.tokenBalances:
    for tokenBalance in bal:
        token = tokenBalance["token"]
        token_list.add(token)


token_file = "data/tokens.csv" 
if os.path.isfile(token_file):
    token_df = pd.read_csv( token_file, dtype={'address':str,'symbol':str } )
    unknown_tokens = token_list.difference( token_df.address.unique() )
    new_rows = [ { 'address': x, 'symbol': '' } for x in unknown_tokens ]
    token_df = pd.concat( [token_df, pd.DataFrame( new_rows ) ] ) 
else:
    token_df = pd.DataFrame( {'address': sorted(list(token_list)), 'symbol': ['' for _ in range(len(token_list))] } )

unknown_symbols = token_df.symbol.isnull()
print( f'Getting symbols for {sum(unknown_symbols)} tokens' )
token_df.loc[unknown_symbols,'symbol'] = token_df[unknown_symbols].address.apply( get_symbol )

token_df.to_csv( token_file, index=False )

