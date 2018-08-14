#!/usr/bin/env python

import sys
import time
import subprocess

from esbtc import DaemonBTC
from esbtc import ElasticsearchBTC
from esbtc import OP_RETURN


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

        txs[vin['txid']].append(i['_source']['tx'])



#count = 0
#opreturns = []
#for i in es.get_opreturn_data():
#
#    # This is just meant to be a status counter for us
#    if not (count % 1000):
#        print(count)
#    count = count + 1
#
#    link = {}
#    if i['_source']['tx'] in txs:
#        link['next'] = txs[i['_source']['tx']]
#
#        temp = {    '_type': 'doc',
#                    '_op_type': 'update',
#                    '_index': "btc-opreturn",
#                    '_id': i['_id'],
#                    'doc': link
#                }
#        opreturns.append(temp)
#
#
#errors = es.update_opreturns(iter(opreturns))
#print("Errors:")
#print(errors)
