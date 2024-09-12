"""
"""

import sys
from solana.rpc.api import Client, Pubkey
from solana.rpc.types import TokenAccountOpts
from spl.token._layouts import MINT_LAYOUT, ACCOUNT_LAYOUT
import base64
import base58
import struct
import json
from urllib.request import Request, urlopen

with open('api_key', 'r') as file:
    api_key = file.read().replace('\n', '')

solana_client = Client(f"https://solana-mainnet.core.chainstack.com/{api_key}")
#solana_client = Client("https://api.mainnet-beta.solana.com")

METADATA_PROGRAM_ID = Pubkey.from_string('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s')
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

def get_symbol( mint_key ):
    try:
        metadata = get_metadata( mint_key )
    except Exception as e:
        print( f'Error in get_symbol({mint_key})' )
        print( e )
        return ""

    if metadata is None:
        return ''

    if 'data' in metadata.keys():
        if 'symbol' in metadata['data']:
            return metadata['data']['symbol']

    return ''

def get_name( mint_key ):
    try:
        metadata = get_metadata( mint_key )
    except Exception as e:
        print( f'Error in get_symbol({mint_key})' )
        print( e )
        return ""

    if metadata is None:
        return ''

    if 'data' in metadata.keys():
        if 'name' in metadata['data']:
            return metadata['data']['name']

    return ''

def get_token_pda(mint_key):
    """
        mint_key - str
    """
    metadata_bytes = bytes(METADATA_PROGRAM_ID)
    key_bytes = bytes(Pubkey.from_string(mint_key))
    return(Pubkey.find_program_address([b'metadata', metadata_bytes,key_bytes],METADATA_PROGRAM_ID)[0])


def unpack_metadata_account(data):
    """
        data - list
        Returns dict
        Taken from https://gist.githubusercontent.com/CrackerHax/61882cf814cde4d9cbc6f5a709e51c34/raw/38d0f11f6f394f7aea0be788f1760f5302b59c91/solana_metadata_assets_from_wallet.py
    """
    if data[0] != 4:
        print( f"Error: data[0] = {data[0]} != 4" )
        print( f"data = {data}" )
        return
    i = 1
    source_account = base58.b58encode(bytes(struct.unpack('<' + "B"*32, data[i:i+32])))
    i += 32
    mint_account = base58.b58encode(bytes(struct.unpack('<' + "B"*32, data[i:i+32])))
    i += 32
    name_len = struct.unpack('<I', data[i:i+4])[0]
    i += 4
    name = struct.unpack('<' + "B"*name_len, data[i:i+name_len])
    i += name_len
    symbol_len = struct.unpack('<I', data[i:i+4])[0]
    i += 4 
    symbol = struct.unpack('<' + "B"*symbol_len, data[i:i+symbol_len])
    i += symbol_len
    uri_len = struct.unpack('<I', data[i:i+4])[0]
    i += 4 
    uri = struct.unpack('<' + "B"*uri_len, data[i:i+uri_len])
    i += uri_len
    fee = struct.unpack('<h', data[i:i+2])[0]
    i += 2
    has_creator = data[i] 
    i += 1
    creators = []
    verified = []
    share = []
    if has_creator:
        creator_len = struct.unpack('<I', data[i:i+4])[0]
        i += 4
        for _ in range(creator_len):
            creator = base58.b58encode(bytes(struct.unpack('<' + "B"*32, data[i:i+32])))
            creators.append(creator)
            i += 32
            verified.append(data[i])
            i += 1
            share.append(data[i])
            i += 1
    primary_sale_happened = bool(data[i])
    i += 1
    is_mutable = bool(data[i])
    metadata = {
        "update_authority": source_account,
        "mint": mint_account,
        "data": {
            "name": bytes(name).decode("utf-8").strip("\x00"),
            "symbol": bytes(symbol).decode("utf-8").strip("\x00"),
            "uri": bytes(uri).decode("utf-8").strip("\x00"),
            "seller_fee_basis_points": fee,
            "creators": creators,
            "verified": verified,
            "share": share,
        },
        "primary_sale_happened": primary_sale_happened,
        "is_mutable": is_mutable,
    }
    return metadata

def get_tokens_by_acct(acct):
    """
        acct - str or Pubkey

        Returns a list of dicts with information about all tokens held by acct
    """
    if isinstance( addr, str ):
        pkey = Pubkey.from_string( acct )
    elif isinstance( addr, Pubkey ):
        pkey = pkey
    else:
        print( f'Unknown type passed to get_tokens_by_acct: {type(pkey)}' )
        return []

    try:
        opts = TokenAccountOpts(program_id=TOKEN_PROGRAM_ID)
        #opts = TokenAccountOpts(mint=Pubkey.from_string('2G5CotQ6Q87yhZxKUWkwLY6Foi12Q3VFQ6KN4nTLbPSz'), encoding="jsonParsed")
    except Exception as e:
        print( f'Error creating opts' )
        print( e )
        return []

    try:
        resp = solana_client.get_token_accounts_by_owner(pkey, opts)
    except Exception as e:
        print( f'Error getting token accounts' )
        print( pkey )
        print( opts )
        print(e)
        return []

    out = []
    for tok in resp.value:
        pk = tok.pubkey
        data = solana_client.get_account_info(pk).value.data
        parsed = ACCOUNT_LAYOUT.parse(data)
        for k in ['mint','owner']:
            parsed[k] = Pubkey( parsed[k] )
        meta = get_metadata(parsed['mint'])
        d = { 'token_address': parsed['mint'], 'acct_address': pk, 'symbol': meta['data']['symbol'], 'amount': parsed['amount'] }
        out.append( d )
    return(out)

def get_metadata(mint_key):
    """
        mint_key - str
    """
    mint_key_str = mint_key
    if isinstance( mint_key, Pubkey ):
        mint_key_str = str(mint_key)
    try:
        data = solana_client.get_account_info(get_token_pda(mint_key_str)).value.data
    except Exception as e:
        print( e )
        return 

    return(unpack_metadata_account(data))
        
def get_nft(mint_key):
    files = {}
    out = {}
    try:
        meta = get_metadata(mint_key)['data']
    except Exception as e:
        print( e )
        return

    if 'uri' in meta.keys():
        try:
            req = Request(meta['uri'])
            token_json = json.loads( urlopen( req ).read() )
        except Exception as e:
            print( e )
            print( f"Error getting metadata from: {meta['uri']}" )
            return     
    else:
        print( f"No uri in metada!" )
        print( meta.keys() )
        return

    try:
        out["name"] = meta["name"]
    except:
        return

    try:
        f = token_json['properties']['files']
    except:
        return(out)
    
    for file in f:
        if(file["type"] == "image/gif" or file["type"] == "image/png" or file["type"] == "fbx"or file["type"] == "video/mp4" ):
            files[file["uri"]] = file["type"]
    out["files"] = files
    out["id"] = mint_key
    return(out)

if __name__ == '__main__':
    addr_pk = Pubkey.from_string('EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm')
    addr = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm'
    assert str(addr_pk) == addr, 'Encoding / decoding failed'

    print( f'Getting metadata for {addr}' )
    print( get_metadata(addr) )

    mad_lads_i= '4X4GaX7fU4MStCcddCmzxjj3tF7So9FgjT1TixUtnXSn'
    print( f'Getting token metadata for {mad_lads_i}' )
    print( get_nft( mad_lads_i ) )

    acct = 'HEGfxcbHPdkpxZJyqPaGevaAkCGfuaB1mHF1cUv2bTGe'
    print( f'Getting all tokens owned by {acct}' )
    print( get_tokens_by_acct( acct ) )


