#!/usr/bin/env python

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from elasticsearch import Elasticsearch
import json
import ast
import time
from Queue import Queue
from threading import Thread

def worker():
    while True:
        i = block_q.get()
        try:
            print("%d/%d"%(i['height'], height))
            es.update(id=block_data['hash'], index="btc-test", doc_type='doc', body={'doc' :block_data, 'doc_as_upsert': True})
        except:
            # Something went wrong, put it back in the queue
            block_q.put(i)

rpc_connection = AuthServiceProxy("http://test:test@127.0.0.1:8332")
es = Elasticsearch(['http://elastic:password@localhost:9200'])

block_q = Queue()

height = rpc_connection.getblockcount()
print(height)
print("---")
for i in range(10):
    t = Thread(target=worker)
    t.daemon = True
    t.start()

for i in range(0, height):
    block = rpc_connection.getblockhash(i)
    block_data = rpc_connection.getblock(block)
    block_data['transactions'] = len(block_data['tx'])
    block_q.put(block_data)

block_q.join()

# Save this for later
#print("---")
#rtx = rpc_connection.getrawtransaction("e0cad5860612c842f0c2a0e6a781249517e0ca7dd5e0df6dad11bcc01a2e04c8")
#tx  = rpc_connection.decoderawtransaction(rtx)
#print(tx)
