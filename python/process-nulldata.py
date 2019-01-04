#!/usr/bin/env python

import sys
import time
import subprocess

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC
from esbtc import OP_RETURN

es = ElasticsearchBTC()
txs = OP_RETURN(es)

# We have to break this up into a bunch of pieces. There's too much data to
# scroll it at once (things fall over)

# Skip to the 200K blocks, there are no OP_RETURN transactions before then
for index in range(2, 6):
    for height in range(0, 100000, 100):

        idx = "btc-transactions-%d" % index
        low = index * 100000 + height
        high = index * 100000 + height + 100

        print(idx)
        print(low)
        print(high)
        for i in es.get_nulldata_transactions(idx, [low, high]):

            # This thing loves to timeout. Rebuilding it every time
            # isn't ideal, we need to fix it someday
            btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332")
            bttx = btcdaemon.get_transaction(i['_source']['txid'])

            #for tx in i['_source']['vout']:
            for tx in bttx['vout']:
                tx_hash = bttx['hash']
                tx_id = bttx['txid']

                #try:
                #    tx_id = i['_source']['txid']
                #except KeyError:
                #    tx_id = tx_hash
                #    print("KeyError: txid missing %s" % tx_hash)
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
                        print("ValueError %s:%s" % (tx_hash, asm))
                    size = len(output)
                    tx_num = tx['n']
                    height = i['_source']['height']
                    #fh = open('/tmp/btc-out', 'wb')
                    #fh.write(output)
                    #size = fh.tell()
                    #fh.close()
                    #filetype = subprocess.check_output(['file', '-b', '/tmp/btc-out'])
                    #filetype = filetype.rstrip()

                    doc = {}

                    if 'vin' in bttx:
                        doc['vin'] = bttx['vin']

                    doc['tx'] = tx_hash
                    doc['txid'] = tx_id
                    doc['n'] = tx_num
                    #doc['type'] = filetype
                    doc['size'] = size
                    doc['height'] = height

                    txs.add_transaction(doc)

# Write whatever is left
es.add_bulk_tx(txs)
