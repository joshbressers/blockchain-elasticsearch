#!/usr/bin/env python

import sys
import time

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC


es = ElasticsearchBTC()

for i in es.get_nonstandard_transactions():
    for tx in i['_source']['vout']:
        tx_hash = i['_source']['hash']
        filenum = 0
        if tx['scriptPubKey']['type'] == "nonstandard":
            output = bytes.fromhex(tx['scriptPubKey']['hex'])
            fh = open('output/%s-%d' % (tx_hash, filenum), 'wb')
            fh.write(output)
            fh.close()
            filenum = filenum + 1
