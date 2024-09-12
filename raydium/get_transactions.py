"""
Get all transactions from a Solana contract.

Loosely inspired by:
https://chainstack.com/solana-python-tutorial-querying-and-analyzing-data-from-raydium/
https://colab.research.google.com/drive/1yzdxqVBRgKRsfLOMPsaoRq4QZAg0lfGb
"""

with open('api_key', 'r') as file:
    api_key = file.read().replace('\n', '')

endPoint = f"https://solana-mainnet.core.chainstack.com/{api_key}"


import json
import solana
import time
import threading
import json
from urllib.request import Request, urlopen
import csv
import os.path
import pandas as pd
import ast

from solana.rpc.api import Client
explorerURLAdd = "https://explorer.solana.com/address/"
explorerURLTx = "https://explorer.solana.com/tx/"

Address_RaydiumAMM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
#Address_liquidityPool = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
#Address_solscan = "https://api.solscan.io/account?address="
RaydiumPubKey = solana.rpc.types.Pubkey.from_string(Address_RaydiumAMM)

http_client = Client(endPoint)

resultArr = []
num_sigs = 50000 
batch_size = 200 #Max 1000 (https://solana.com/docs/rpc/http/getsignaturesforaddress#parameters)
tx_file = "data/raydiumTxs.csv"
sig_file = "data/raydiumSigs.csv"
error_file = "errors.txt"

if os.path.isfile(sig_file):
    with open(sig_file) as f:
        known_sigs = f.read().splitlines()
    last_signature = known_sigs[-1]
else:
    known_sigs = []
    last_signature = None

def getTxDetail(txSignature):
    """
    txSignature - string

    Gets transaction details from the Solana RPC
    Stores details in global array, resultArr
    """
    txSignature2 = solana.transaction.Signature.from_string(txSignature)
    try:
        tx = http_client.get_transaction(txSignature2,max_supported_transaction_version=0)
    except Exception as e:
        with open( error_file, "a" ) as f:
            f.write( txSignature + "\n" )
        return

    tx = json.loads(tx.to_json())
    postTokenBalances = tx["result"]["meta"]["postTokenBalances"]
    preTokenBalances = tx["result"]["meta"]["preTokenBalances"]
    d = tx['result']
    d.update( { 'signature': txSignature } )
    resultArr.append(d)
    if (postTokenBalances != preTokenBalances):
        print(explorerURLTx+txSignature)

def getTxSigs( PubKey, batch_size, num_sigs, last_signature):
    """
        PubKey 
        batch_size
        num_sigs
        lastSignature
    """
    print("getting transactions")
    if isinstance( last_signature, str ):
        lastSignature = solana.transaction.Signature.from_string(last_signature)
    else:
        lastSignature = None
    rounds = 0
    txCount = 0
    while(True):
        print("round-"+str(rounds+1))
        try:
            txs = http_client.get_signatures_for_address(PubKey,limit=batch_size,before=lastSignature).to_json()
        except Exception as e:
            print( f"Error getting signatures" )
            print( e )
            time.sleep(5)
            continue
        txs = json.loads(txs)["result"]
        if len( txs ) == 0:
            break
        print("processing signatures")
        signatures = [o["signature"] for o in txs]
        with open( sig_file, 'a' ) as f:
            for s in signatures:
                f.write( f"{s}\n" )

        signatures = [s for s in signatures if s not in known_sigs]
        threads = list()
        for signature in signatures:
            txCount += 1
            x = threading.Thread(target=getTxDetail, args=(signature,))
            threads.append(x)
            x.start()
        for index, thread in enumerate(threads):
            thread.join()
        if(txCount >= num_sigs):
            break
        else:
            rounds += 1
            lastSignature = solana.transaction.Signature.from_string(txs[-1]["signature"])
        time.sleep(3)

    if os.path.isfile(error_file):
        with open(error_file) as f:
            error_sigs = f.read().splitlines()
        try:
            os.remove(error_file)
        except Exception as e:
            print( e )

        error_sigs = [s for s in error_sigs if s != '']
        if len(error_sigs) > 0:
            print( f"Regrabbing data for {len(error_sigs)} signatures" )
            for s in error_sigs:
                getTxDetail(s)


json_cols = ['transaction.signatures','transaction.message.accountKeys','meta.preBalances','meta.postBalances','meta.preTokenBalances','transaction.message.instructions','meta.innerInstructions','meta.logMessages','meta.preTokenBalances','meta.postTokenBalances','meta.loadedAddresses.writable' ]
tx_df = pd.read_csv( tx_file,  converters={ c : ast.literal_eval for c in json_cols } )
known_sigs = set( tx_df.signature.unique() )

getTxSigs( RaydiumPubKey, batch_size, num_sigs, last_signature )
df = pd.json_normalize( resultArr )
print( f'Got {df.shape[0]} new transactions' )

if df.shape[0] > 0:
    if os.path.isfile(tx_file):
        json_cols = ['transaction.signatures','transaction.message.accountKeys','meta.preBalances','meta.postBalances','meta.preTokenBalances','transaction.message.instructions','meta.innerInstructions','meta.logMessages','meta.preTokenBalances','meta.postTokenBalances','meta.loadedAddresses.writable' ]
        old_df = pd.read_csv( tx_file,  converters={ c : ast.literal_eval for c in json_cols } )
        df = pd.concat( [df,old_df] )

    print( f'Writing {df.shape[0]} total transactions to {tx_file}' )
    df.to_csv( tx_file, index=False )

