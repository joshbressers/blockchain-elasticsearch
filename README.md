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
```

When you run the bitcoin client, use the -txindex option
`./bitcoin-qt -txindex`
The first time you run this, it's going to take a crazy long time to sync
the network.

You should be able to just run "get-blocks.py" and wait. A long time.

You can find some price data here
https://api.coindesk.com/v1/bpi/historical/close.json?start=2010-07-17&end=2018-09-07


## Processing the nulldata
Once you have all the blocks and transactions syncd, first run the
process-nulldata.py script. It takes time to run. This will build an index
of all nulldata transactions.

Once that script is done, create a directory named sorted with
subdirectories a-f and 0-9 in it. Run the link-nulldata.py script to link
the transactions together that matter and write them out to disk.

If you want to put ASCII content into elasticsearch for parsing you can use
the find-text.py script. It needs to be run from inside the sorted
directory.
