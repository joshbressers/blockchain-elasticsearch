#!/usr/bin/env python

import sys
import time

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC


btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332")
es = ElasticsearchBTC('https://elastic:L1dlgKQJKVySKGsUe8BHqkaj@a00b3a264d104fc2ae01d6682729be83.us-east-1.aws.found.io:9243/')

height = btcdaemon.get_max_block()

size = 0
if len(sys.argv) > 1:
    size = int(sys.argv[1])

if size == -1:
    size = es.get_max_block() + 1

for i in range(size, height + 1):
        block = btcdaemon.get_block(i)
        print("block %d/%d"%(block['height'], height))
        es.add_block(block)

        # Add transactions
        txs = btcdaemon.get_block_transactions_bulk(i)
        print("  Transactions: %i" % len(txs))
        errors = es.add_bulk_tx(txs)
        print("  %i errors" %  len(errors))
