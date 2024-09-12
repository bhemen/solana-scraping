# -*- coding: utf-8 -*-
"""Analyze Raydium AMM.ipynb
https://chainstack.com/solana-python-tutorial-querying-and-analyzing-data-from-raydium/
https://colab.research.google.com/drive/1yzdxqVBRgKRsfLOMPsaoRq4QZAg0lfGb
"""

import pandas as pd
import json
import ast

def processRaydiumTx(tx):
    #print( tx.keys() )
    txSignature = tx["transaction.signatures"][0]
    txSender = tx["transaction.message.accountKeys"][0]
    blockTime = tx["blockTime"]
    slot = tx["slot"]
    postTokenBalances = tx["meta.postTokenBalances"]
    preTokenBalances = tx["meta.preTokenBalances"]
    tokenBalances = []
    for pre, post in zip(preTokenBalances, postTokenBalances):
        change = 0
        if not (post["uiTokenAmount"]["uiAmount"] is None or pre["uiTokenAmount"]["uiAmount"] is None):
            change = post["uiTokenAmount"]["uiAmount"] - pre["uiTokenAmount"]["uiAmount"]
        owner = pre["owner"]
        token = pre["mint"]
        if(change != 0):
            tokenBalances.append({
                "owner":owner,
                "token":token,
                "change":change
            })
    result = pd.Series( { 
            "txSignature":txSignature,
            "sender" : txSender,
            "blockTime" : blockTime,
            "slot" : slot,
            "tokenBalances" : tokenBalances
        } )
    return result

json_cols = ['transaction.signatures','transaction.message.accountKeys','meta.preBalances','meta.postBalances','meta.preTokenBalances','transaction.message.instructions','meta.innerInstructions','meta.logMessages','meta.preTokenBalances','meta.postTokenBalances','meta.loadedAddresses.writable' ]
df = pd.read_csv( "data/raydiumTxs.csv",  converters={ c : ast.literal_eval for c in json_cols } )

processed = df.apply( processRaydiumTx, axis=1 )
processed.to_csv( "data/raydiumTxsProcessed.csv", index=False )

print( processed.head() )

#for c in df.columns:
#    print( "=====================" )
#    print( c )
#    print( df[c].head() )
