import requests
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import time
import re

def file_exists_with_name(folder_path, filename_without_extension):
    # Convert folder_path to a Path object if it's a string
    folder = Path(folder_path)
    
    # Check if the folder exists and is a directory
    if not folder.is_dir():
        raise ValueError(f"The path {folder_path} is not a valid directory.")
    
    # Use glob to find any file that matches the name without extension
    for file in folder.glob(f'{filename_without_extension}.*'):
        # If any match is found, return True
        if file.is_file():
            return True
    # If no match is found, return False
    return False


def fetch_image( token_address, uri, retry=0, backoff=1):
    if not isinstance( uri, str ):
        tqdm.write( f'Error uri for {token_address} is {uri}' )
        return False

    if file_exists_with_name( image_dir, token_address ): #Skip images we've already downloaded
        return True

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

    if not ipfs:
        #for example https://bafkreibk3covs5ltyqxa272uodhculbr6kea6betidfwy3ajsav2vjzyum.ipfs.nftstorage.link
        match = re.match( r'https://([a-zA-Z0-9]+)\.ipfs\.([a-zA-Z0-9\-\.]+)', uri )
        if match:
            ipfs = True
            cid = match.group(1)

    response = None
    if ipfs:
        gateways = ['https://ipfs.io/ipfs/','https://gateway.pinata.cloud/ipfs/','https://cloudflare-ipfs.com/ipfs/']
        for gateway in gateways:
            full_uri = f"{gateway}{cid}"
            try:
                response = requests.get(full_uri,timeout=10)
                response.raise_for_status()
                break
            except Exception as e:
                tqdm.write(f"Failed to get image from {full_uri}")
                response = None
        if (response is None) and (retry < max_retries):
            time.sleep(backoff)
            tqdm.write(f"Attempt {retry + 1} of {max_retries} for {full_uri}")
            return fetch_image( token_address, uri, retry=retry+1, backoff=backoff*2 )
        elif response is None:
            tqdm.write(f"Reached max attempts ({max_retries}) for CID {cid}" )
            return False
    else: 
        if uri.startswith("http"):
            try:
                response = requests.get(uri,timeout=10)
                response.raise_for_status()
            except Exception as e:
                if retry < max_retries:
                    time.sleep(backoff)
                    tqdm.write(f"Attempt {retry + 1} of {max_retries} for {uri}: Error - {type(e).__name__}, {e}")
                    return fetch_image( token_address, uri, retry=retry+1, backoff=backoff*2 )
                else:
                    tqdm.write(f"Failed to get image from {uri} after {max_retries} attempts")
                    return False

    if response is None:
        return None
    content_type = response.headers.get('Content-Type', '')
    if content_type: 
        file_extension = content_type.split('/')[-1]  # Use the RHS of the Content-Type
    else:
        file_extension = 'bin'  # Fallback extension if Content-Type is missing or empty
        tqdm.write(f"Warning: No Content-Type for {url}. Using '.bin' as the extension.")

    # Update the file path with the determined extension
    file_path = f"data/images/{token_address}.{file_extension}"

    with open(file_path, 'wb') as file:
        file.write(response.content)
    return True

image_dir = 'data/images'
max_retries = 5

df = pd.read_csv('data/token_metadata.csv')
tqdm.pandas()
df.progress_apply( lambda row: fetch_image( row['address'], row['image'] ), axis=1 )


