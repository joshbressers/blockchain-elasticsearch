# blockchain-elasticsearch
Project for putting Blockchain data into Elasticsearch

## Getting started

Start up elasticsearch and Kibana
Paste the contents of mapping.txt into the Kibana dev tools

Your bitcoin.conf should look like this

```
server=1
rpcuser=test
rpcpassword=test
rpcallowip=127.0.0.1
```

When you run the bitcoin client, use the -txindex option
`./bitcoin-qt -txindex`

You should be able to just run "get-blocks.py" and wait
