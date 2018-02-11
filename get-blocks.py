#!/usr/bin/env python

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionTimeout
import sys
import socket
from Queue import Queue
from threading import Thread

def block_worker():
    while True:
        try:
            i = block_q.get()
            print("block %d/%d"%(i['height'], height))
            try:
                es.get(index="btc-blocks", doc_type="doc", id=i['hash'])
                # It exists if this returns, let's skip it
            except NotFoundError:
                # We need to add this block
                es.update(id=i['hash'], index="btc-blocks", doc_type='doc',
body={'doc' :i, 'doc_as_upsert': True}, request_timeout=30)
        except KeyboardInterrupt as e:
            sys.exit(1)

        except socket.timeout:
            # Something went wrong, put it back in the queue
            block_q.put(i)


rpc_connection = AuthServiceProxy("http://test:test@127.0.0.1:8332")
es = Elasticsearch(['http://elastic:password@localhost:9200'])

block_q = Queue()
count_q = Queue()

height = rpc_connection.getblockcount()

for i in range(10):
    t = Thread(target=block_worker)
    t.daemon = True
    t.start()

size = 0
if len(sys.argv) > 1:
    size = int(sys.argv[1])

for i in range(size, height):
    count_q.put(i)

# Don't try to thread this, bad things happen if we hit the bitcoin server
# with more than one client
while not count_q.empty():
    try:
        i = count_q.get()
        block = rpc_connection.getblockhash(i)
        block_data = rpc_connection.getblock(block)
        block_data['transactions'] = len(block_data['tx'])
        block_q.put(block_data)
    except:
        # We need to catch certain exceptions
        # probably
        raise

block_q.join()

# Save this for later
#print("---")
#rtx = rpc_connection.getrawtransaction("e0cad5860612c842f0c2a0e6a781249517e0ca7dd5e0df6dad11bcc01a2e04c8")
#tx  = rpc_connection.decoderawtransaction(rtx)
#print(tx)
