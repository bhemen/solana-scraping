import requests 
import pandas as pd

with open('api_key', 'r') as file:
    api_key = file.read().replace('\n', '')
  
v1_endpoint = "https://graphql.bitquery.io"
v2_endpoint = "https://streaming.bitquery.io/graphql"
eap_endpoint = "https://streaming.bitquery.io/eap"

#https://docs.bitquery.io/docs/examples/Solana/Solana-Raydium-DEX-API/#latest-price-of-a-token
dex_query = """ 
query ListDEXes {
  Solana {
    DEXTrades(limitBy: {by: Trade_Dex_ProgramAddress, count: 1}) {
      Trade {
        Dex {
          ProgramAddress
          ProtocolFamily
          ProtocolName
        }
      }
    }
  }
}
"""
 

def run_query(query):
    headers = {'X-API-KEY': api_key}
    request = requests.post(eap_endpoint, json={'query': query }, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed and return code is {}.    {}'.format(request.status_code, query))

resp = run_query(dex_query)
print( resp )
print( resp.keys() )

rows = []
for d in resp['data']['Solana']['DEXTrades']:
    dex = d['Trade']['Dex']
    r = { a : dex[a] for a in ['ProgramAddress','ProtocolFamily','ProtocolName'] }
    rows.append(r)

df = pd.DataFrame( rows )
df.to_csv( "data/DEXes.csv", index=False )
