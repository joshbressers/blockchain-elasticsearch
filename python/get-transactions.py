#!/usr/bin/env python

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionTimeout
import sys
import socket
import time
from queue import Queue
from queue import Empty
from threading import Thread

def block_worker():
    while True:
        try:
            i = tx_q.get(timeout=10)
            #print("block %d/%d"%(i['height'], height))
            the_index = "btc-transactions-%d" % (i['height'] / 100000)
            es.update(id=i['hash'], index=the_index, doc_type='doc',
                body={'doc' :i, 'doc_as_upsert': True},
                request_timeout=30)
            #try:
            #    es.get(index=the_index, doc_type="doc", id=i['hash'])
            #    # It exists if this returns, let's skip it
            #except NotFoundError:
            #    # We need to add this transaction
            #    print(i['hash'])
            #    es.update(id=i['hash'], index=the_index, doc_type='doc',
            #        body={'doc' :i, 'doc_as_upsert': True},
            #        request_timeout=30)
        except KeyboardInterrupt as e:
            sys.exit(1)

        except Empty:
            # We're done here
            return

        except socket.timeout:
            # Something went wrong, put it back in the queue
            tx_q.put(i)


rpc_connection = AuthServiceProxy("http://test:test@127.0.0.1:8332")
#es = Elasticsearch(['http://elastic:password@localhost:9200'])
es = Elasticsearch(['https://elastic:L1dlgKQJKVySKGsUe8BHqkaj@a00b3a264d104fc2ae01d6682729be83.us-east-1.aws.found.io:9243/'])


tx_q = Queue(maxsize=1000)
count_q = Queue()

height = rpc_connection.getblockcount()

for i in range(15):
    t = Thread(target=block_worker)
    t.daemon = True
    t.start()

# We have to skip block 0
size = 1
if len(sys.argv) > 1:
    size = int(sys.argv[1])

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

