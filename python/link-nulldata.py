#!/usr/bin/env python

import sys
import os
import time
import subprocess

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC
from esbtc import OP_RETURN

sys.setrecursionlimit(3000)

def get_ids(txs, the_id):
    ids = [the_id]

    child = the_id
    while True:
        try:
            child = txs[child][0]
            ids.append(child)
        except KeyError:
            return ids

    return ids

es = ElasticsearchBTC()

txs = {}

for i in es.get_opreturn_data():

    if not (len(txs) % 1000):
        print(len(txs))

    # coinbase transactions don't have a vin
    if 'vin' not in i['_source']:
        continue

    for vin in i['_source']['vin']:
        if vin['txid'] not in txs:
            txs[vin['txid']] = []

        txs[vin['txid']].append(i['_source']['txid'])

# Remove all the children
parents = set(txs)
for i in txs:

    for child in txs[i]:
        try:
            parents.remove(child)
        except KeyError:
            pass

for p in parents:

    if os.path.isfile('sorted/%s/%s' % (p[0], p)):
        print('sorted/%s/%s exists, skipping' % (p[0], p))
        continue

    needed_ids = get_ids(txs, p)

    fh = open('sorted/%s/%s' % (p[0], p), 'wb')
    tx_data = es.get_transactions(needed_ids)
    for t in tx_data:

        found = False
        if t is None:
            continue
        if t['vout'] is None:
            continue
        for tx in t['vout']:
            # Let's just parse one OP_RESERVED
            if found:
                next
            if tx['scriptPubKey']['type'] == "nulldata":
                asm = tx['scriptPubKey']['asm']
                if not asm.startswith('OP_RETURN '):
                    next
                # Yank the OP_RETURN
                asm = asm[10:]

                # Just skip these
                if asm.startswith('OP_RESERVED'):
                    next
                # Remove spaces (sometimes it happens)
                asm = asm.replace(' ', '')
                if len(asm) % 2:
                    asm = asm + '0'
                try:
                    output = bytes.fromhex(asm)
                except ValueError:
                    print("%s:%s" % (t, asm))
                fh.write(output)

                found = True
    fh.close()
