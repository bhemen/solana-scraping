import asyncio

from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport
import os
import pandas as pd
import json
from datetime import datetime

#If we call this from cron, it will run it from a different CWD, so relative paths won't work
dir_path = os.path.dirname(os.path.realpath(__file__))
output_dir = 'data'

def handle_result( result ):
    rows = []
#    print( json.dumps( result.data['Solana'], indent=2 ) )
    for r in result.data['Solana']['TokenSupplyUpdates']:
        base_level = ['Amount','PostBalance']
        d = { k : r['TokenSupplyUpdate'][k] for k in base_level }
        d.update( r['TokenSupplyUpdate']['Currency'] )
        d['TokenCreator'] = d['TokenCreator']['Address']
        d['Time'] = r['Block']['Time']
        d['Height'] = r['Block']['Height']
        rows.append(d )

    df = pd.DataFrame( rows )
    #print( "=======================" )
    #print( df.head() )
    #print( "=======================" )

    today = datetime.now().strftime("%Y%m%d")
    
    # Generate filename for current day
    
    outfile = f'pump_mints_{today}.csv'
    full_filename = os.path.join( dir_path, output_dir, outfile )
    n = len(df)
   
    if n > 0:
        if os.path.exists(full_filename):
            # If file exists, append without header
            df.to_csv(full_filename, mode='a', header=False, index=False)
            if n == 1:
                print(f"1 mint appended to {outfile}")
            else:
                print(f"{n} mints appended to {outfile}")
        else:
            # If file doesn't exist, create new file with header
            df.to_csv(full_filename, index=False)
            if n == 1:
                print(f"{outfile} created with {n} row")
            else:
                print(f"{outfile} created with {n} rows")

async def main():

    try:
        with open(f'{dir_path}/access_token', 'r') as file:
            access_token = file.read().rstrip()
    except Exception as e:
        access_token = None
        print( e )

    websocket_url = f"wss://streaming.bitquery.io/eap?token={access_token}"

    transport = WebsocketsTransport(
        url=websocket_url,
        headers={"Sec-WebSocket-Protocol": "graphql-ws"})

    await transport.connect()
    print("Connected")
	
    subscription_query = """
        subscription {
          Solana {
            TokenSupplyUpdates(
              where: {Instruction: {Program: {Address: {is: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"}, Method: {is: "create"}}}}
            ) {
              TokenSupplyUpdate {
                Amount
                Currency {
                  Symbol
                  ProgramAddress
                  PrimarySaleHappened
                  Native
                  Name
                  MintAddress
                  MetadataAddress
                  Key
                  IsMutable
                  Fungible
                  EditionNonce
                  Decimals
                  Wrapped
                  VerifiedCollection
                  Uri
                  UpdateAuthority
                  TokenStandard
                  TokenCreator {
                    Address
                  }
                }
                PostBalance
              }
              Block {
                Date
                Height
                Time
              }
            }
          }
        }
    """

    # Define the subscription query
    query = gql(subscription_query)

    async def subscribe_and_print():
        try:
            async for result in transport.subscribe(query):
                handle_result(result)
        except asyncio.CancelledError:
            print("Subscription cancelled.")

    # Run the subscription and stop after 100 seconds
    try:
        await asyncio.wait_for(subscribe_and_print(), timeout=100)
    except asyncio.TimeoutError:
        print("Stopping subscription after 100 seconds.")

    # Close the connection
    await transport.close()
    print("Transport closed")


# Run the asyncio event loop
asyncio.run(main())


