# Python scripts for interacting with the Solana blockchain

~~~
    pip3 install -r requirements.txt
~~~

* [leader_schedule.py](leader_schedule.py) Every ``slot'' in Solana has a leader who is allowed to produce a block in that slot.  This script collects all the slot leader for every slot, and stores the data in [data/solana_leaders.csv](data/solana_leaders.csv).
* [simple_usdc.py](simple_usdc.py) This script shows how to grab basic information about a token, in this case Circle's USDC.
* [token_examples.py](token_examples.py) This script shows how to create and transfer tokens
