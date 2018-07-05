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

def block_worker():
    old_index = 0
    fh = open("%s/0.json" % sys.argv[1], 'w')
    while True:
        try:
            i = tx_q.get(timeout=10)
            print("block %d/%d"%(i['height'], height))
            the_index = "btc-transactions-%d" % (i['height'] / 100000)

            if (i['height'] / 100000) != old_index:
                old_index = old_index + 1
                fh.close()
                fh = open("%s/%s.json" % (sys.argv[1], old_index), 'w')

            fh.write('{"index": {"_index": "%s", "_type": "doc", "_id": "%s"}}\n' % (the_index, i['hash']))
            fh.write(json.dumps(i))
            fh.write('\n')

        except KeyboardInterrupt as e:
            sys.exit(1)

        except Empty:
            # We're done here
            fh.close()
            return


if len(sys.argv) <= 1:
    sys.exit("Usage: %s <output file>" % sys.argv[0])

rpc_connection = AuthServiceProxy("http://test:test@127.0.0.1:8332")

tx_q = Queue(maxsize=1000)
count_q = Queue()

height = rpc_connection.getblockcount()

for i in range(1):
    t = Thread(target=block_worker)
    t.daemon = True
    t.start()

# We have to skip block 0
size = 1
if len(sys.argv) > 2:
    size = int(sys.argv[2])

for i in range(size, height + 1):
    count_q.put(i)

# Don't try to thread this, bad things happen if we hit the bitcoin server
# with more than one client
while not count_q.empty():
    try:
        i = count_q.get()
        block = rpc_connection.getblockhash(i)
        block_data = rpc_connection.getblock(block)

        for tx in block_data['tx']:
            # Load the transaction
            rtx = rpc_connection.getrawtransaction(tx)
            dtx  = rpc_connection.decoderawtransaction(rtx)
            dtx['height'] = block_data['height']
            dtx['block'] = block_data['hash']
            dtx['time'] = block_data['time']
            tx_q.put(dtx)
    except socket.timeout:
        # Put the count back in the queue
        count_q.put(i)
    except:
        # We need to catch certain exceptions
        # probably
        raise

while not tx_q.empty():
    time.sleep(2)

