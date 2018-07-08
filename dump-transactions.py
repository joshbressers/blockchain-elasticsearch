#!/usr/bin/env python

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
import sys
import socket
import time
from Queue import Queue
from Queue import Empty
from threading import Thread
import simplejson as json
import sys


if len(sys.argv) <= 1:
    sys.exit("Usage: %s <output file>" % sys.argv[0])

rpc_connection = AuthServiceProxy("http://test:test@127.0.0.1:8332")

height = rpc_connection.getblockcount()

# We have to skip block 0
size = 1
if len(sys.argv) > 2:
    size = int(sys.argv[2])

write_count = 0
old_index = 0
fh = open("%s/0.json" % sys.argv[1], 'w')

for i in range(size, height + 1):
    try:
        block = rpc_connection.getblockhash(i)
        block_data = rpc_connection.getblock(block)

        for tx in block_data['tx']:
            # Load the transaction
            rtx = rpc_connection.getrawtransaction(tx)
            dtx  = rpc_connection.decoderawtransaction(rtx)
            dtx['height'] = block_data['height']
            dtx['block'] = block_data['hash']
            dtx['time'] = block_data['time']

            i = dtx
            if (write_count % 1000) == 0:
                print("block %d/%d"%(i['height'], height))
            the_index = "btc-transactions-%d" % (i['height'] / 100000)

            if (((write_count % 20000) == 0) and write_count > 0):
                write_count = 0
                old_index = old_index + 1
                fh.write('\n')
                fh.close()
                fh = open("%s/%s.json" % (sys.argv[1], old_index), 'w')

            fh.write('{"update": {"_index": "%s", "_type": "doc", "_id": "%s"}}\n' % (the_index, i['hash']))
            doc_json = json.dumps(i)
            fh.write('{"doc" : %s, "doc_as_upsert" : true}\n' % doc_json)
            write_count = write_count + 1

    except KeyboardInterrupt as e:
        sys.exit(1)
