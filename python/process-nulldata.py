#!/usr/bin/env python

import sys
import time
import subprocess

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC
from esbtc import OP_RETURN

es = ElasticsearchBTC()
txs = OP_RETURN(es)
btcdaemon = DaemonBTC("http://test:test@127.0.0.1:8332")

# We have to break this up into a bunch of pieces. There's too much data to
# scroll it at once (things fall over)

indices = sorted(es.get_transactions_indices())

if len(sys.argv) > 1:
    indices = indices[int(sys.argv[1]):]

all_txs = 0
total_loop = 0

print("Indices to scan")
for one_index in indices:
    current_count = es.count_nulldata_transactions(one_index)['count']
    all_txs = all_txs + current_count
    print("{:s} {:d} documents".format(one_index, current_count))
print("---------------")

for one_index in indices:

    idx = one_index
    total_tx = es.count_nulldata_transactions(idx)['count']

    loop_count = 0

    for i in es.get_nulldata_transactions(idx):
        loop_count = loop_count + 1
        total_loop = total_loop + 1

        percent = (loop_count/total_tx) * 100

        from_len = len(str(all_txs))
        allstr = "{num:{fill}{width}}".format(num=total_loop, fill=' ', width=from_len)

        from_len = len(str(total_tx))
        loopstr = "{num:{fill}{width}}".format(num=loop_count, fill=' ', width=from_len)

        # By moving the cursor here we preserve the last printed line in
        # case of an exception
        print('', end='\r')
        print("{:s} : {:s}/{:d} {:05.3f}% - All: {:s}/{:d}".format(idx, loopstr, total_tx, percent, allstr, all_txs), end='')


        bttx = i['_source']

        for tx in bttx['vout']:
            tx_hash = bttx['hash']
            tx_id = bttx['txid']

            if tx['scriptPubKey']['type'] == "nulldata":

                tx_num = tx['n']
                height = i['_source']['height']


                doc = {}

                doc['tx'] = tx_hash
                doc['txid'] = tx_id
                doc['n'] = tx_num
                doc['height'] = height

                txs.add_transaction(doc)

# Write whatever is left
es.add_bulk_tx(txs)
