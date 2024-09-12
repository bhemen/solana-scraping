from solana.rpc.api import Client, Pubkey, Keypair
from spl.token._layouts import ACCOUNT_LAYOUT
from spl.token.instructions import get_associated_token_address
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.client import Token
from solana.rpc.types import TxOpts
import solders.rpc.errors
import json
import sys

sk_file = "sol_sk_py.txt" #List of secret keys in hex format, one per line.  We store them so that we don't have to keep requesting new SOL from the faucet
token_file = "token_py.txt" #List of token mints in JSON format.  This saves us the trouble of creating a new token mint onchain every time we want to test a transfer
url = "https://api.devnet.solana.com";
web3 = Client(url)

def readAccounts(sk_file):
    """
        Reads secret keys (in hex) from sk_file (one key per line)
        Returns a list of Keypair objects generated from these secret keys
        If the file does not exist or is empty, it returns an empty list

        Args:
            sk_file - filename
        Returns: 
            List of Keypair objects 
    """
    try:
        with open(sk_file,"r") as f:
            sks = json.load(f)

        accounts = [Keypair.from_bytes(bytes.fromhex(sk)) for sk in sks]
    except:
        accounts = []
    return accounts

def genAccounts(sk_file,num_accounts):
    """
        Reads accounts from sk_file (using readAccounts)
        If the number of keys in the file is less than the specified num_accounts, 
        generate the remaining accounts, and write the secret keys back to the file

        Args:
            sk_file - filename
            num_accounts - integer 
        Returns:
            List of keypair objects

    """
    accounts = readAccounts(sk_file)
    if len(accounts) >= num_accounts:
        return accounts

    accounts += [Keypair() for _ in range(num_accounts-len(accounts))]
    
    with open(sk_file,"w") as f:
        f.write( json.dumps( [bytes( account ).hex() for account in accounts], indent=2 ) )
    
    return accounts

def fundAccounts( connection, accounts ):
    """
        Ensure that every account in accounts has at least min_balance SOL
        If the balance is less, it will use connection to request an airdrop of SOL (you cannot request airdrops of SOL on mainnet, so this only works on devnet)

        This function has been failing a lot recently -- it seems that Solana is requiring a CAPTCHA te get funds, so you'll probably have to do this manually

        Args:
            connection - RPC connection (to devnet)
            accounts - list of account objects
        Returns:
            None
    """
    min_balance = 5*10**8 #Remember 10^9 Lamports per SOL https://solana.com/docs/terminology#lamport
    for account in accounts:
        if connection.get_balance(account.pubkey()).value < min_balance:
            print( f"Requesting airdrop for account: {account.pubkey()}" )      
            failed = False
            try:
                resp = connection.request_airdrop(account.pubkey(),min_balance, opts = TxOpts( skip_confirmation=False ) )
                print( resp )
                try:
                    if isinstance( resp, solders.rpc.errors.InternalErrorMessage ):
                        failed = True
                        print( f"Airdrop error" )
                        print( resp )
                    else:
                        print( resp )
                        print( dir( resp ) )
                        sig = resp.value.Signature
                        print( f'Airdrop sig = {sig}' )
                except Exception as ee:
                    failed = True
                    print( ee ) 
            except Exception as e:
                failed = True
                print( f"request_airdrop failed" )
                print( e )
            if failed:
                print( f"Airdrop failed for {account.pubkey()}" )
                print( f"Try requesting an airdrop manually from https://faucet.solana.com" )


def readTokens(conn,token_file,payer):
    """
        Reads a list of token mints from a JSON file, and returns a list of token objects

        Args:
            conn - Solana RPC client connection
            token_file - name of token file
            payer - Keypair object (this should be the account that pays fees for the transfers.  It is likely the same account that is sending the tokens, but it doesn't have to be)
        Returns:
            List of Token objects
    """
    try:
        with open(token_file,"r") as f:
            tokens = json.load(f)

        tokens = [Token(conn=conn,pubkey = Pubkey.from_bytes(bytes.fromhex(token['pubkey'])),program_id = Pubkey.from_bytes(bytes.fromhex(token['program_id'])), payer=payer) for token in tokens]
    except Exception as e:
        print( e )
        tokens = []
    return tokens

def genTokens(conn, token_file, minter, payer, num_tokens):
    """
        Creates a list of token mints.  It tries to read them from token_file, if token_file is empty, 
        or there are not enough tokens already known, then it generates more and writes them to token_file

        Args:
            conn - Solana RPC client connection
            token_file - name of token file (in JSON format)
            minter - Pubkey object
            payer - Keypair object 
            num_tokens - Number of token mints to generate

        Returns:
            List of Token objects
    """
    tokens = readTokens(conn,token_file,payer)
    if len(tokens) == 1:
        print( f"Read {len(tokens)} token" )
    else:
        print( f"Read {len(tokens)} tokens" )

    if len(tokens) >= num_tokens:
        return tokens

    for i in range(num_tokens-len(tokens)):
        print( f"Creating token {i+1} of {num_tokens-len(tokens)}" )
        try:
            token = Token.create_mint(
                conn = conn,
                payer = payer,
                mint_authority = minter,
                decimals = 0,
                program_id = TOKEN_PROGRAM_ID
                )
            tokens.append(token)
        except Exception as e:
            print( 'Failed to create token' )
            print( e )

    with open(token_file,"w") as f:
        f.write( json.dumps( [{'pubkey':bytes(token.pubkey).hex(), 'program_id':bytes(token.program_id).hex() } for token in tokens], indent=2 ) )
    
    return tokens
            
def getTokenAccounts(token,user_pk):
    """
        Gets a list of associated token accounts for a given user

        Args:
            token - Token object
            user_pk - Pubkey object
        Returns:
            Dictionary of the form: { pubkey : balance } where pubkey is the pubkey of an associated token account, and balance is the token balance on that account
    """
    try:
        resp = token.get_accounts_by_owner(user_pk).value
    except Exception as e:
        print( f"Failed to get token accounts for {user_pk}" )
        print( e )
        return {}

    #You may think that the 'owner' of the associated token account should be the user, but it's not, it's the token program
    for r in resp:
        rr = web3.get_account_info( r.pubkey ).value
        if str(rr.owner) != str(TOKEN_PROGRAM_ID):
            print( f"Associated Token Account at {r.pubkey} for token {token.pubkey} and user {user_pk} is owned by {rr.owner}" )

    token_accounts = { str(r.pubkey): float(ACCOUNT_LAYOUT.parse( r.account.data ).amount) for r in resp}

    return token_accounts

#############################################
#Create accounts
#############################################
accounts = genAccounts(sk_file,3)

payer = accounts[0]
sender = accounts[1]
receiver = accounts[2]

fundAccounts( web3, accounts )

balances = { str(account.pubkey()) : web3.get_balance(account.pubkey()).value for account in accounts }
for pk,bal in balances.items():
    print( f"{pk}: {bal}" )

#############################################
#Create Token
#############################################

if balances[str(payer.pubkey())] == 0:
    print( "ERROR" )
    print( f"{payer.pubkey()} has no SOL" )
    print( f"Can't mint tokens" )
    print( f"Try requesting an airdrop manually from https://faucet.solana.com" )
    print( f"Exiting" )
    sys.exit(1)

tokens = genTokens(web3,token_file,payer.pubkey(),payer, 1)

for token in tokens:
    resp = web3.get_account_info(token.pubkey)
    print( f"Mint owner = {resp.value.owner}" )
    
#############################################
#Mint tokens
#############################################

token = tokens[0]

print( f'Token address = {token.pubkey}' )

mint_amount = 100
transfer_amount = 25

sender_accounts = getTokenAccounts( token, sender.pubkey() )
receiver_accounts = getTokenAccounts( token, receiver.pubkey() )

if len(sender_accounts) == 0:
    print(f"Creating token account for {sender.pubkey()}")
    sender_token_pubkey = token.create_associated_token_account(sender.pubkey())
    assert sender_token_pubkey == get_associated_token_address( sender.pubkey(), token.pubkey )
    sender_accounts = getTokenAccounts( token, sender.pubkey() )

if len(receiver_accounts) == 0:
    print(f"Creating token account for {receiver.pubkey()}")
    receiver_token_pubkey = token.create_associated_token_account(receiver.pubkey())
    receiver_accounts = getTokenAccounts( token, receiver.pubkey() )

sender_token_pubkey = Pubkey.from_string( max(sender_accounts, key=sender_accounts.get) )
receiver_token_pubkey = Pubkey.from_string( min(receiver_accounts, key=receiver_accounts.get) )

print( f"sender_token_pubkey = {str(sender_token_pubkey)}" )
print( f"receiver_token_pubkey = {str(receiver_token_pubkey)}" )

token_balances = getTokenAccounts(token,sender.pubkey())
#print( json.dumps(token_balances,indent=2) )

if token_balances[str(sender_token_pubkey)] < transfer_amount:
    print( f"Minting {mint_amount} tokens to {sender_token_pubkey} owned by {sender.pubkey()}" )
    resp = token.mint_to(
        dest = sender_token_pubkey,
        mint_authority = payer,
        amount = mint_amount,
        opts = TxOpts( skip_confirmation=False )
    )
    sig = resp.value
    print( f"View mint transaction at" )
    print( f"https://explorer.solana.com/tx/{sig}?cluster=devnet" )

    sender_balance = float(token.get_balance(sender_token_pubkey).value.amount)
    receiver_balance = float(token.get_balance(receiver_token_pubkey).value.amount)

    print( f"After minting: Sender token balance = {sender_balance}" )
    print( f"After minting: Receiver token balance = {receiver_balance}" )

#############################################
#Transfer tokens
#############################################

send_tokens = readTokens( web3, token_file, sender )
send_token = send_tokens[0]

assert send_token.pubkey == token.pubkey, f'Error incorrect token'
assert sender_token_pubkey == get_associated_token_address( sender.pubkey(), token.pubkey )

sender_balance = float(send_token.get_balance(sender_token_pubkey).value.amount)
receiver_balance = float(send_token.get_balance(receiver_token_pubkey).value.amount)

print( f"Before transfer: Sender token balance = {sender_balance}" )
print( f"Before transfer: Receiver token balance = {receiver_balance}" )

if sender_balance >= transfer_amount:
    print( f'Transferring {transfer_amount} from {sender_token_pubkey} to {receiver_token_pubkey}' )
    resp = send_token.transfer(
        source = sender_token_pubkey,
        dest = receiver_token_pubkey,
        owner = sender,
        amount = transfer_amount,
        opts = TxOpts( skip_confirmation=False )
    )
    sig = resp.value
    print( f"View transfer transaction at" )
    print( f"https://explorer.solana.com/tx/{sig}?cluster=devnet" )
else:
    print( "Mint failed" )
    print( f'Cannot transfer {transfer_amount} from {sender_token_pubkey} to {receiver_token_pubkey}' )
    print( "Sender balance too low" )
    print( json.dumps(getTokenAccounts(send_token,sender.pubkey()),indent=2) )

sender_balance = float(send_token.get_balance(sender_token_pubkey).value.amount)
receiver_balance = float(send_token.get_balance(receiver_token_pubkey).value.amount)

print( f"After transfer: Sender token balance = {sender_balance}" )
print( f"After transfer: Receiver token balance = {receiver_balance}" )

