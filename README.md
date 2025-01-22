# Python scripts for interacting with the Solana blockchain

~~~
    pip3 install -r requirements.txt
~~~

* [leader_schedule.py](leader_schedule.py) Every ``slot'' in Solana has a leader who is allowed to produce a block in that slot.  This script collects all the slot leader for every slot, and stores the data in [data/solana_leaders.csv](data/solana_leaders.csv).
* [simple_usdc.py](simple_usdc.py) This script shows how to grab basic information about a token, in this case Circle's USDC.
* [token_examples.py](token_examples.py) This script shows how to create and transfer tokens
* [get_metadata.py](get_metadata.py) Gets token metadata from the Solana blockchain and writes it to [data/token_metadata.csv](data/token_metadata.csv)
* [get_images_from_ipfs.py](get_images_from_ipfs.py) This does *not* interact with the Solana blockchain, instead it reads URIs from [data/token_metadata.csv](data/token_metadata.csv) and tries to grab the corresponding images from IPFS.  The output data is too large to store in github, but the files are available on [box](https://upenn.box.com/s/gltwet0xe7ha3rltvfyt7wy1fkj5ymam)
