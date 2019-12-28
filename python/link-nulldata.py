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

lower = None
higher = None
if len(sys.argv) > 1:
    lower = int(sys.argv[1])
if len(sys.argv) > 2:
    higher = int(sys.argv[2])

if lower is None:
    lower = 220000
if higher is None:
    higher = 700000

for j in range(lower, higher, 10):
    print('', end='\n')
    txs = {}
    height = {}

    for i in es.get_opreturn_data(j, j+10):

        # coinbase transactions don't have a vin
        if 'vin' not in i['_source']:
            continue

        for vin in i['_source']['vin']:
            if vin['txid'] not in txs:
                txs[vin['txid']] = []

            txs[vin['txid']].append(i['_source']['txid'])
            height[vin['txid']] = i['_source']['height']

    # Find all the parent transactions
    parents = set(txs)
    for i in txs:

        for child in txs[i]:
            try:
                parents.remove(child)
            except KeyError:
                pass

    total_parents = len(parents)
    current_parents = 0
    btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332")
    tx_data = []
    for p in parents:
        current_parents = current_parents + 1

        print('', end='\r')
        print("{:d}  {:d}/{:d}        ".format(j, current_parents, total_parents), end='')

        if not os.path.isdir('sorted/%s' % height[p]):
            os.mkdir('sorted/%s' % height[p])

        needed_ids = get_ids(txs, p)
        filename = p

        fh = open('sorted/%s/%s' % (height[p], filename), 'ab')

        # This https connection likes to timeout as sometimes we have LOTS of
        # waiting. This is an insane hack, but meh
        for attempt in range(10):
            try:
                tx_data = btcdaemon.get_transactions(needed_ids)
            except:
                btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332", timeout=10)
                next
            else:
                break

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
                    if 'asm' not in tx['scriptPubKey']:
                        continue
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
