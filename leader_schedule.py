"""
    This script helps measure decentralization in Solana

    This script creates the file data/solana_leaders.csv
    The output has the following three columns:

    leader - The address of the leader
    epoch - The epoch number
    num_slots - The number of slots assigned to that leader in that epoch

"""

from solana.rpc.api import Client
import time
import os
import pandas as pd
import csv
from tqdm import tqdm

url = "https://api.mainnet-beta.solana.com";
web3 = Client(url)
outfile = "data/solana_leaders.csv"

slots_per_epoch = web3.get_epoch_schedule().value.slots_per_epoch #Should be 432,000
latest_epoch = web3.get_epoch_info().value.epoch

max_wait = 32 #Longest time to wait on RPC before giving up

columns = ['leader','epoch','num_slots'] #Columns to write to CSV

known_epochs = set()

if not os.path.exists( outfile ):
    with open( outfile, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow( columns )
else:
    try:
        df = pd.read_csv( outfile )
        known_epochs = set( df.epoch.unique() )
        del df
    except Exception as e:
        print( e )

current_wait = base_wait = 2

epochs = list( set( range( latest_epoch + 1 ) ).difference(known_epochs) )
epochs.sort( reverse=True )

progress = tqdm(epochs)
for epoch in progress:
    progress.set_description( f'Processing {epoch}' )
    try:
        schedule = web3.get_leader_schedule(epoch*slots_per_epoch).value
    except Exception as e:
        print( f'Error!' )
        print( e )
        if current_wait > max_wait:
            progress.set_description( f"Error: Skipping epoch {epoch}" )
            current_wait = base_wait
            epoch -= 1
        else:
            progress.set_description( f"Retrying epoch {epoch} in {current_wait}" )
            time.sleep(current_wait)
            current_wait *= 2
        continue

    if schedule is None:
        if current_wait > max_wait:
            progress.set_description( f"Skipping epoch {epoch}" )
            current_wait = base_wait
            epoch -= 1
        else:
            progress.set_description( f"Retrying epoch {epoch} in {current_wait}" )
            time.sleep(current_wait)
            current_wait *= 2
        continue
    
    for leader, slots in schedule.items():
        row = [ leader, epoch, len(slots) ]
        with open( outfile, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow( row )

        current_wait = base_wait

    epoch -= 1
    time.sleep(1)

