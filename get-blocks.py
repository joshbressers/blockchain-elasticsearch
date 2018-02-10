#!/usr/bin/env python

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionTimeout
import json
import ast
import time
import sys
from Queue import Queue
from threading import Thread

def block_worker():
    while True:
        try:
            i = block_q.get()
            print("\033[3;0Hblock %d/%d"%(i['height'], height))
            try:
                es.get(index="btc-blocks", doc_type="doc", id=i['hash'])
                # It exists if this returns, let's skip it
                fails = 0
            except NotFoundError:
                # We need to add this block
                es.update(id=i['hash'], index="btc-blocks", doc_type='doc',
body={'doc' :i, 'doc_as_upsert': True}, request_timeout=30)
        except KeyboardInterrupt as e:
            sys.exit(0)

        except ConnectionTimeout:
            # Something went wrong, put it back in the queue
            block_q.put(i)

def count_worker():
    while True:
        try:
            i = count_q.get()
            print("\033[1;0Hcount %d/%d"%(i, height))
            block = rpc_connection.getblockhash(i)
            block_data = rpc_connection.getblock(block)
            block_data['transactions'] = len(block_data['tx'])
            block_q.put(block_data)
        except KeyboardInterrupt as e:
            sys.exit(0)
            # Something went wrong, put it back in the queue
            #count_q.put(i)

rpc_connection = AuthServiceProxy("http://test:test@127.0.0.1:8332")
es = Elasticsearch(['http://elastic:password@localhost:9200'])

block_q = Queue()
count_q = Queue()

height = rpc_connection.getblockcount()

# Clear the screen
print(chr(27) + "[2J")

for i in range(0, height):
    count_q.put(i)

# If we use more than one thread here, crazy things happen
for i in range(1):
    c = Thread(target=count_worker)
    c.daemon = True
    c.start()


for i in range(10):
    t = Thread(target=block_worker)
    t.daemon = True
    t.start()



count_q.join()
block_q.join()

# Save this for later
#print("---")
#rtx = rpc_connection.getrawtransaction("e0cad5860612c842f0c2a0e6a781249517e0ca7dd5e0df6dad11bcc01a2e04c8")
#tx  = rpc_connection.decoderawtransaction(rtx)
#print(tx)
