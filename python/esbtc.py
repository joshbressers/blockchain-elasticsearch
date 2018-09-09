
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionTimeout
import elasticsearch
import elasticsearch.helpers
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

import os

class ElasticsearchBTC:
    "Class for querying the Elasticsearch BTC instance"

    def __init__(self, url=None):

        if url is None:
            self.url = os.environ['ESURL']
        else:
            self.url = url
        self.es = Elasticsearch([self.url], http_compress = True)
        self.size = None

    def get_block(self, block=None, height=None):
        result = {}
        if block:
            result = self.es.search(index="btc-blocks-*", body={"query": { "match": { "_id": block }}})
        elif height:
            result = self.es.search(index="btc-blocks-*", body={"query": { "match": { "height": height }}})

        # We're just going to assume neither of these can return
        # multiple things
        if len(result['hits']['hits']) == 0:
            return None
        else:
            return result['hits']['hits'][0]['_source']

    def get_transactions(self, tx):

        unsorted = {}

        # Use batches of 20
        for batch_range in range(0, len(tx), 20):

            result = self.es.search(index="btc-transactions-*",
                                    body={"size": 50,
                                          "query": {
                                            "terms": {
                                              "txid": tx[batch_range:batch_range+20]
                                            }
                                          }
                                         }
                                   )

            if len(result['hits']['hits']) == 0:
                return None

            # Collect the results
            for i in result['hits']['hits']:
                unsorted[i['_source']['txid']] = i['_source']

        # Return the results in the right order
        output = []
        for i in tx:
            if i in unsorted:
                output.append(unsorted[i])
            else:
                import pdb; pdb.set_trace()
        return output

    def get_transaction(self, tx):

        result = self.es.search(index="btc-transactions-*", body={"query": { "match": { "txid": tx }}})

        # We're just going to assume neither of these can return
        # multiple things
        if len(result['hits']['hits']) == 0:
            return None
        else:
            return result['hits']['hits'][0]['_source']

    def get_block_transactions(self, block):
            result = self.es.search(index="btc-transactions-*", body={"query": { "match": { "block": block }}})

            txs = []
            for i in result['hits']['hits']:
                txs.append(i['_source'])
            return txs

    def get_nonstandard_transactions(self):
            query = { "_source": ["hash", "vout.scriptPubKey.hex", "vout.scriptPubKey.type"], "query" : { "match": { "vout.scriptPubKey.type": "nonstandard" } } }

            return elasticsearch.helpers.scan(self.es, index="btc-transactions-*", query=query, scroll='1m')

    def get_nulldata_transactions(self, index, height_range):
            # This is a mess. Apologies if you're looking at this

            l = height_range[0]
            h = height_range[1]
            query = { "_source": ["hash",
                                  "height",
                                  "txid",
                                  "vin.txid",
                                  "vout.scriptPubKey.asm",
                                  "vout.scriptPubKey.type",
                                  "vout.n"
                                 ],
                      "query" : {
                        "bool": {
                          "must": [
                            {"term": { "vout.scriptPubKey.type": "nulldata" }},
                            {"range" : { "height" : { "gte" : l, "lte" :  h}}}
                          ]
                        }
                      }
                    }

            return elasticsearch.helpers.scan(self.es, index=index, query=query, scroll='5m')

    def get_opreturn_data(self):

            query = { "_source": ["tx",
                                  "n",
                                  "txid",
                                  "vin.txid",
                                 ],
                      "query" : {
                        "match_all" : {}
                      }
                    }

            return elasticsearch.helpers.scan(self.es, index="btc-opreturn", query=query, scroll='5m')

    def update_opreturns(self, the_iter):

        errors = []

        for ok, item in elasticsearch.helpers.streaming_bulk(self.es, the_iter, max_retries=2):
            if not ok:
                errors.append(item)

        return errors

    def add_opreturn(self, data):

            my_id = "%s-%s" % (data['tx'], data['n'])

            self.es.update(id=my_id, index="btc-opreturn", doc_type='doc', body={'doc' :data, 'doc_as_upsert': True}, request_timeout=30)

    def add_block(self, block):
        "Add a block. Do nothing if the block already exists"

        the_index = "btc-blocks-%d" % (block['height'] / 100000)
        try:
            self.es.get(index=the_index, doc_type="doc", id=block['hash'])
            # It exists if this returns, let's skip it
        except NotFoundError:
            # We need to add this block
            self.es.update(id=block['hash'], index=the_index, doc_type='doc', body={'doc' :block, 'doc_as_upsert': True}, request_timeout=30)

    def add_transaction(self, tx):
        "Add a transaction. Do nothing if the block already exists"

        the_index = "btc-transactions-%d" % (tx['height'] / 100000)
        try:
            self.es.get(index=the_index, doc_type="doc", id=tx['hash'])
            # It exists if this returns, let's skip it
        except NotFoundError:
            # We need to add this transaction
            self.es.update(id=tx['hash'], index=the_index, doc_type='doc', body={'doc' :tx, 'doc_as_upsert': True}, request_timeout=30)

    def add_price(self, date, price):
        "Add the price for a given timestamp"
        price_data = { 'date': date, 'price': price }
        self.es.update(id=date, index="btc-price", doc_type='doc', body={'doc' :price_data, 'doc_as_upsert': True}, request_timeout=30)

    def get_max_block(self):
        "Get the largest block in the system"

        if self.size is None:
            query = {'sort': [{'height': 'desc'}], 'size': 1, 'query': {'match_all': {}}, '_source': ['height']}
            res = self.es.search(index="btc-blocks-*", body=query)
            self.size = res['hits']['hits'][0]['_source']['height']

        return self.size

    def add_bulk_tx(self, data_iterable):
        "Do some sort of bulk thing with an iterable"

        errors = []

        for ok, item in elasticsearch.helpers.streaming_bulk(self.es, data_iterable, max_retries=2):
            if not ok:
                errors.append(item)

        return errors


class DaemonBTC:

    def __init__(self, url):
        self.rpc = AuthServiceProxy(url)

        self.height = self.rpc.getblockcount()


    def get_block(self, i):
        block = self.rpc.getblockhash(i)
        block_data = self.rpc.getblock(block)
        block_data['transactions'] = len(block_data['tx'])
        del(block_data['tx'])

        return block_data

    def get_transaction(self, tx):
        rtx = self.rpc.getrawtransaction(tx)
        dtx = self.rpc.decoderawtransaction(rtx)

        return dtx

    def get_block_transactions(self, block):
        blockhash = self.rpc.getblockhash(block)
        block_data = self.rpc.getblock(blockhash)

        transactions = []

        rtx = self.rpc.batch_([["getrawtransaction", t] for t in block_data['tx']])
        dtx = self.rpc.batch_([["decoderawtransaction", t] for t in rtx])

        for tx in dtx:
            tx['height'] = block_data['height']
            tx['block'] = block_data['hash']
            tx['time'] = block_data['time']
            for i in tx['vin']:
                if 'scriptSig' in i:
                    # We can't use this data, let's get rid of it
                    del(i['scriptSig'])
            transactions.append(tx)

        return transactions

    def get_block_transactions_bulk(self, block):
        "Return an iterable object for bulk transactions"

        transactions = self.get_block_transactions(block)
        tx = Transactions()
        for i in transactions:
            tx.add_transaction(i)

        return tx

    def get_max_block(self):
        return self.rpc.getblockcount()

class Transactions:

    def __init__(self):
        self.transactions = []
        self.current = -1

    def add_transaction(self, tx):
        temp = {    '_type': 'doc',
                    '_op_type': 'update',
                    '_index': "btc-transactions-%d" % (tx['height'] / 100000),
                    '_id': tx['hash'],
                    'doc_as_upsert': True,
                    'doc': tx
                }

        self.transactions.append(temp)


    def __next__(self):
        "handle a call to next()"

        self.current = self.current + 1
        if self.current >= len(self.transactions):
            raise StopIteration

        return self.transactions[self.current]

    def __iter__(self):
        "Just return ourself"
        return self

    def __len__(self):
        return len(self.transactions)

class OP_RETURN:

    def __init__(self, es):
        self.transactions = []
        self.current = -1
        self.es_handle = es

    def add_transaction(self, tx):
        temp = {    '_type': 'doc',
                    '_op_type': 'update',
                    '_index': "btc-opreturn",
                    '_id': "%s-%s" % (tx['tx'], tx['n']),
                    'doc_as_upsert': True,
                    'doc': tx
                }

        self.transactions.append(temp)

        if len(self.transactions) > 1000:
            self.es_handle.add_bulk_tx(self)
            self.transactions = []
            self.current = -1


    def __next__(self):
        "handle a call to next()"

        self.current = self.current + 1
        if self.current >= len(self.transactions):
            raise StopIteration

        return self.transactions[self.current]

    def __iter__(self):
        "Just return ourself"
        return self

    def __len__(self):
        return len(self.transactions)
