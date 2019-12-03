# blockchain-elasticsearch
Project for putting Blockchain data into Elasticsearch

## Getting started

Start up elasticsearch and Kibana
You will need to add a mapping for every json file in the mappings
directory.

Your bitcoin.conf should look like this

```
server=1
rpcuser=test
rpcpassword=test
rpcallowip=127.0.0.1
txindex=1
```

Then run the bitcoin client
`./bitcoind`
The first time you run this, it's going to take a crazy long time to sync
the network. Like weeks or maybe even a month.

You will need to pip install the certifi, elasticsearch, and
python-bitcoinrpc packages.

You should be able to just run "get-blocks.py" and wait. A long time.

There are two ways to run get-blocks.py. You can run it with no switches
and it will load all blocks and transactions into Elasticsearch. This will
depend heavily on the size of your cluster. The code is designed to work on
spinning disks if needed, but it needs nearly a terabyte of space. You can
also run get-blocks.py with the -b option. This will only store the block
data. It's currently pretty small, less than a gig. It's not particularly
fast, but should load in a week or two on most setups.

You can load price data by running `add-price.py all`. This will load all
the historical data. Future runs should omit the all option.

## Processing the nulldata
If you want to process the nulldata stored in the blockchain, you will need
to store all the transactions.

Once you have all the blocks and transactions syncd, first run the
process-nulldata.py script. It takes time to run. This will build an index
of all nulldata transactions.

Once that script is done, create a directory named sorted. Run the
link-nulldata.py script to link the transactions together that matter and
write them out to disk. This process will save millions of files to the
disk. You've been warned.

If you want to put ASCII content into elasticsearch for parsing you can use
the find-text.py script. It needs to be run from inside the sorted
directory.
