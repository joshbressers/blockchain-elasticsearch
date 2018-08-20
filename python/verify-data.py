#!/usr/bin/env python

import sys
import time

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC


btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332")
es = ElasticsearchBTC()

height = btcdaemon.get_max_block()

size = 1
if len(sys.argv) > 1:
    size = int(sys.argv[1])

if len(sys.argv) > 2:
    height = int(sys.argv[2])

if size == -1:
    size = es.get_max_block() + 1

for i in range(size, height + 1):
        block = es.get_block(height=i)
        txs = es.get_block_transactions(block['hash'])
        if block['transactions'] == len(txs):
            pass
        else:
            print("Bad block %d/%d"%(block['height'], height))

            # Add transactions
            txs = btcdaemon.get_block_transactions_bulk(i)
            print("  Transactions: %i" % len(txs))
            errors = es.add_bulk_tx(txs)
            print("  %i errors" %  len(errors))
