#!/usr/bin/env python

import sys
import getopt
import time
import socket

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC
import logging


btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332")
es = ElasticsearchBTC()

tracer = logging.getLogger('elasticsearch')
tracer.setLevel(logging.CRITICAL)

height = btcdaemon.get_max_block()

size = 0
transactions = True

# Command line parsing magic
try:
    opts, args = getopt.getopt(sys.argv[1:],"b1",["blocksonly"])
except getopt.GetoptError:
    print("Usage: ")
    print(' %s [--blocksonly,-b] [min] [max]' % sys.argv[0])
    sys.exit(2)

if (('-b', '') in opts) or (('--blocksonly', '') in opts):
    transactions = False

if (('-1', '') in opts):
    size = es.get_max_block() + 1

if len(args) > 0:
    size = int(args[0])

if len(args) > 1:
    height = int(args[1])


for i in range(size, height + 1):
        #print("block %d/%d"%(block['height'], height))
        print("block %d/%d"%(i, height))
        block = btcdaemon.get_block(i)

        if transactions is True:
            # Add transactions
            while True:
                try:
                    txs = btcdaemon.get_block_transactions_bulk(i)
                    break
                except socket.timeout:
                    btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332")
            print("  Transactions: %i" % len(txs))
            errors = es.add_bulk_tx(txs)
            print("  %i errors" %  len(errors))

        es.add_block(block)
