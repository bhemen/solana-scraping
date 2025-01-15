from solana.rpc.api import Client
from solders.pubkey import Pubkey
import base58
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import get_associated_token_address
import requests

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

def get_token_metadata(token_address: str, rpc_url: str = "https://api.mainnet-beta.solana.com"):
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
    metadata_info = client.get_account_info(metadata_account)
    
    if not metadata_info.value:
        raise ValueError("Metadata account not found")
    
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
            "name": name,
            "symbol": symbol,
            "uri": uri
        }

        print( metadata )
        
        # If there's a URI, fetch additional metadata
        if uri.startswith("http"):
            try:
                additional_metadata = requests.get(uri).json()
                metadata["additional_metadata"] = additional_metadata
            except Exception as e:
                print(f"Warning: Could not fetch additional metadata from URI: {e}")
        
        return metadata
        
    except Exception as e:
        raise ValueError(f"Error parsing metadata: {e}")

# Example usage
if __name__ == "__main__":
    # Example token address (replace with your token's mint address)
    token_address = "98mb39tPFKQJ4Bif8iVg9mYb9wsfPZgpgN1sxoVTpump"  # USDC on Solana
    
    try:
        metadata = get_token_metadata(token_address)
        print(f"Token Metadata: {metadata}")
    except Exception as e:
        print(f"Error: {e}")
