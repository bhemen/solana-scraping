from solana.rpc.api import Client
from solders.pubkey import Pubkey
import base58
from spl.token.constants import TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID
from spl.token.instructions import get_associated_token_address
import requests
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import csv
import time

def bytes_to_string( byte_array ):
    bs = byte_array.strip(b'\x00')
    null_pos = bs.find(b'\x00')
    if null_pos != -1:
        bs = bs[:null_pos]
    try:
        s = bs.decode('utf-8')
    except Exception as e:
        print( f'Error in bytes_to_string' )
        print( f'bs = {bs}' )
        print( e )
        return ""

    return s

def fetch_metadata( uri, retry=0, backoff=1):
    if uri.startswith('ipfs://'):
        ipfs = True
        cid = uri[7:]
    else:
        ipfs = False
    try:
        ipfs = True
        cid = uri.split("/ipfs/")[1]
    except IndexError:
        ipfs = False

    if ipfs:
        gateways = ['https://ipfs.io/ipfs/','https://gateway.pinata.cloud/ipfs/','https://cloudflare-ipfs.com/ipfs/']
        for gateway in gateways:
            full_uri = f"{gateway}{cid}"
            try:
                response = requests.get(full_uri,timeout=10)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                print(f"Warning: Could not fetch additional metadata from {full_uri}")
                print( e )
        if retry < 5:
            time.sleep(backoff)
            return fetch_metadata( uri, retry=retry+1, backoff=backoff*2 )
        else:
            return {}
    else: 
        if uri.startswith("http"):
            try:
                response = requests.get(uri,timeout=10)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                print(f"Warning: Could not fetch additional metadata from {uri}")
                print( e )
                if retry < 5:
                    time.sleep(backoff)
                    return fetch_metadata( uri, retry=retry+1, backoff=backoff*2 )
                else:
                    return {}
        return {}
    

def get_token_metadata(token_address: str, rpc_url: str = "https://api.mainnet-beta.solana.com", retry=1, backoff=2):
    """
    Fetch metadata for a Solana token using its mint address.
    
    Args:
        token_address: The mint address of the token
        rpc_url: The Solana RPC endpoint to use
    
    Returns:
        dict: Token metadata including name, symbol, URI, and additional metadata if available
    """
    # Initialize Solana client
    client = Client(rpc_url)
    
    # Convert address string to Pubkey
    token_pubkey = Pubkey.from_string(token_address)
    
    # Get Metaplex metadata account
    METADATA_PROGRAM_ID = Pubkey.from_string("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")
    
    # Derive metadata account address
    metadata_account = Pubkey.find_program_address(
        seeds=[
            bytes("metadata", "utf8"),
            bytes(METADATA_PROGRAM_ID),
            bytes(token_pubkey)
        ],
        program_id=METADATA_PROGRAM_ID
    )[0]
    
    # Get metadata account info
    try:
        metadata_info = client.get_account_info(metadata_account)
    except Exception as e:
        time.sleep(1)
        if retry < 5:
            time.sleep(backoff)
            return get_token_metadata( token_address, rpc_url, retry+1, backoff=backoff*2 )
        else:
            print( f"get_account_info({metadata_account}) failed after 5 retries" )
            print( e )
    
    if not metadata_info.value:
        print( f"No metaplex data for {token_pubkey}" )
        raise ValueError("No metadata found")

    # Parse metadata
    data = metadata_info.value.data
    
    if not data:
        raise ValueError("No metadata found")

    try:
        # Skip the first byte (version) and the update authority (32 bytes)
        current_index = 1 + 32
        
        # Skip mint address (32 bytes)
        current_index += 32
        
        # Get name length (4 bytes) and name
        name_length = int.from_bytes(data[current_index:current_index+4], "little")
        current_index += 4
        name = bytes_to_string( data[current_index:current_index+name_length] )
        current_index += 32  # Name padding is 32 bytes
        
        # Get symbol length (4 bytes) and symbol
        symbol_length = int.from_bytes(data[current_index:current_index+4], "little")
        current_index += 4
        symbol = bytes_to_string( data[current_index:current_index+symbol_length] )
        current_index += 8  # Symbol padding is 8 bytes
        
        # Get URI length (4 bytes) and URI
        uri_length = int.from_bytes(data[current_index:current_index+4], "little")
        current_index += 4
        uri = bytes_to_string( data[current_index:current_index+uri_length] )
        
        metadata = {
            "address": token_address,
            "name": name,
            "symbol": symbol,
            "uri": uri
        }

        # If there's a URI, fetch additional metadata
        try:
            additional_metadata = fetch_metadata(uri)
        except Exception as e:
            print( f"Failed to fetch metadata from {uri}" )
            return metadata 
        for e in extra_cols:
            if e in additional_metadata.keys():
                metadata.update( {e: additional_metadata[e].rstrip().replace('\n','')} )
        
        return metadata
        
    except Exception as e:
        print( f"Error parsing metadata: {data}" )
        raise ValueError(f"Error parsing metadata: {e}")

provider='chainstack'

endPoint = "https://api.mainnet-beta.solana.com"

if provider == 'alchemy':
    with open('alchemy_api_key', 'r') as file:
        api_key = file.read().replace('\n', '')
    endPoint = f"https://solana-mainnet.g.alchemy.com/v2/{api_key}"
if provider == 'chainstack':
    with open('chainstack_api_key', 'r') as file:
        api_key = file.read().replace('\n', '')
    endPoint = f"https://solana-mainnet.core.chainstack.com/{api_key}"

data_path = Path("bitquery/data")
csv_files = list(data_path.glob("token_prices*.csv"))
token_addresses = set()
for f in csv_files:
    df = pd.read_csv( f )
    if 'TokenAddress' in df.columns:
        token_addresses = token_addresses.union( df.TokenAddress )

outfile = "data/token_metadata.csv"

try:
    df = pd.read_csv(outfile)
    known_addresses = df.address.unique()
    print( f'{len(known_addresses)} known addresses' )
except Exception as e:
    known_addresses = []

unknown_addresses = token_addresses.difference(known_addresses)
extra_cols = ['description','image','createdOn']
columns = ['address','name','symbol','uri'] + extra_cols

if not Path(outfile).is_file():
    with open( outfile, 'w' ) as f:
        writer = csv.DictWriter( f, fieldnames=columns )
        writer.writeheader()

for a in tqdm(unknown_addresses):
    try:
        metadata = get_token_metadata(a,rpc_url=endPoint)
        time.sleep(1)
    except Exception as e:
        time.sleep(2)
        print(f"Error getting token metadata for {a}")
        print( e )
        continue

    with open( outfile, 'a') as f:
        writer = csv.DictWriter( f, fieldnames=columns )
        writer.writerow(metadata)

