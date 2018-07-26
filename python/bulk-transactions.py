#!/usr/bin/env python

import sys
import time

from esbtc import ElasticsearchBTC
from esbtc import DaemonBTC


rpc = DaemonBTC("http://test:test@127.0.0.1:8332")
es = ElasticsearchBTC('https://elastic:L1dlgKQJKVySKGsUe8BHqkaj@a00b3a264d104fc2ae01d6682729be83.us-east-1.aws.found.io:9243/')

height = rpc.get_max_block()

# We have to skip block 0
size = 1
if len(sys.argv) > 1:
    size = int(sys.argv[1])

for i in range(size, height + 1):

    print("%i/%i" % (i, height))
    txs = rpc.get_block_transactions_bulk(i)
    errors = es.add_bulk_tx(txs)
    print("%i errors" % len(errors))
