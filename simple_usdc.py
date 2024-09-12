"""
    Example of how to get basic info about a token (e.g. USDC)
"""

from solana.rpc.api import Client, Pubkey
import spl.token.instructions as spl_token
from spl.token.constants import TOKEN_PROGRAM_ID
#from spl.token.client import Token

sk_file = "sol_sk_py.txt"
token_file = "token_py.txt"
#https://docs.solana.com/cluster/rpc-endpoints
url = "https://api.mainnet-beta.solana.com"
#url = "https://api.devnet.solana.com";
web3 = Client(url)

#https://explorer.solana.com/address/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
usdc_address = Pubkey.from_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

try:
    resp = web3.get_account_info(usdc_address).value
    owner = resp.owner
    #print( resp )
    assert str(owner) == str(TOKEN_PROGRAM_ID)
except Exception as e:
    print( e )
    owner = 'Unknown'

print( f'Owner = {owner}' )

try:
    resp = web3.get_token_supply(usdc_address).value
    supply = int( resp.amount ) / (10**(int(resp.decimals)))
    #print( resp )
except Exception as e:
    print( e )
    supply = 'Unknown'

print( f"Supply = {supply}" )

try:
    resp = web3.get_token_largest_accounts(usdc_address).value
    print( resp )
except Exception as e:
    print( 'failed to call get_token_largest_accounts' )
    print( e )

circle = Pubkey.from_string('7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE')
circle_ata = spl_token.get_associated_token_address( circle, usdc_address ) #Every address has a unique "associated" token account for each mint

from spl.token.client import Token

usdc_token = Token( 
                conn = web3,  
                pubkey = usdc_address,
                program_id = TOKEN_PROGRAM_ID,
                payer = None )

try:
    resp = usdc_token.get_accounts_by_owner(circle).value
    #print( resp )
    token_accounts = { str(r.pubkey): float(usdc_token.get_balance(r.pubkey).value.amount) for r in resp}
    print( f"Circle has {len(token_accounts)} accounts holding USDC" )
    for acct, bal in token_accounts.items():
        if str(acct) == str(circle_ata):
            print( f"{acct} : {bal}  ** Associated Token Account" )
        else:
            print( f"{acct}: {bal}" )
except Exception as e:
    print( f"Failed to get token accounts for {circle}" )
    print( e )

