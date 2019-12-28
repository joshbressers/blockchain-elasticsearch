
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.exceptions import TransportError
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
        self.es = Elasticsearch([self.url], http_compress = True, timeout=60)
        self.size = None

    def get_transactions_indices(self):
        return self.es.indices.get('btc-transactions-*').keys()

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
                pass
                # Sometimes crazy things happen
                #import pdb; pdb.set_trace()
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

    def get_block_transactions_number(self, block):
            result = self.es.search(index="btc-transactions-*", body={"query": { "match": { "block": block }}})

            return result['hits']['total']

    def get_nonstandard_transactions(self):
            query = { "_source": ["hash", "vout.scriptPubKey.hex", "vout.scriptPubKey.type"], "query" : { "match": { "vout.scriptPubKey.type": "nonstandard" } } }

            return elasticsearch.helpers.scan(self.es, index="btc-transactions-*", query=query, scroll='1m')

    def count_nulldata_transactions(self, index):
        result = self.es.count(index=index,
                               body={
                                      "query": {
                                        "term": { "vout.scriptPubKey.type": "nulldata" }
                                      }
                                    }
                              )
        return result


    def get_nulldata_transactions(self, index):
            # This is a mess. Apologies if you're looking at this

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
                            {"term": { "vout.scriptPubKey.type": "nulldata" }}
                          ]
                        }
                      }
                    }

            return elasticsearch.helpers.scan(self.es, index=index, query=query, scroll='5m')

    def get_opreturn_data(self, bottom = None, top = None):

            query = { "_source": ["tx",
                                  "height",
                                  "n",
                                  "txid",
                                  "vin.txid",
                                 ],
                      "query" : {
                        "match_all" : {}
                      }
                    }

            if bottom is not None and top is not None:
                query['query'] = {"range" : { "height" : { "gte" : bottom, "lte" :  top}}}

            return elasticsearch.helpers.scan(self.es, index="btc-opreturn", query=query, size=100, scroll='1m')

    def get_opreturn_tx(self, tx):

        result = self.es.search(index="btc-opreturn", body={"query": {
"match": { "txid": { "query": tx }}}})

        # We're just going to assume neither of these can return
        # multiple things
        if len(result['hits']['hits']) == 0:
            return None
        else:
            return result['hits']['hits']

    def set_opreturn_tx_parent(self, tx):

        my_id = tx['_id']
        data = tx['_source']
        data['is_parent'] = True

        self.es.update(id=my_id, index="btc-opreturn", doc_type='doc', body={'doc' :data}, request_timeout=10)

    def add_opreturn_tx_child(self, parent_txid, child_txid):

        tx = self.get_opreturn_tx(parent_txid)

        if tx is None:
            return None

        my_id = tx[0]['_id']
        data = tx[0]['_source']
        if 'children' in data and child_txid not in data['children']:
            data['children'].append(child_txid)
        else:
            data['children'] = [child_txid]

        self.es.update(id=my_id, index="btc-opreturn", doc_type='doc', body={'doc' :data}, request_timeout=10)

    def update_opreturns(self, the_iter):

        errors = []

        for ok, item in elasticsearch.helpers.streaming_bulk(self.es, the_iter, max_retries=2):
            if not ok:
                errors.append(item)

        return errors

    def add_opreturn(self, data):

            my_id = "%s-%s" % (data['tx'], data['n'])

            #self.es.update(id=my_id, index="btc-opreturn", doc_type='doc', body={'doc' :data, 'doc_as_upsert': True}, request_timeout=30)
            self.es.update(id=my_id, index="btc-opreturn", body={'doc' :data, 'doc_as_upsert': True}, request_timeout=30)

    def add_block(self, block, force_add=False):
        "Add a block. Do nothing if the block already exists"

        read_index = "btc-blocks-*"
        the_index = "btc-blocks"

        exists = False
        try:
            #self.es.get(index=the_index, doc_type="doc", id=block['hash'])
            self.es.get(index=read_index, id=block['hash'])
            exists = True
        except NotFoundError:
            # We need to add this block
            exists = False

        if exists is False or force_add is True:
            #self.es.update(id=block['hash'], index=the_index, doc_type='doc', body={'doc' :block, 'doc_as_upsert': True}, request_timeout=30)
            self.es.update(id=block['hash'], index=the_index, body={'doc' :block, 'doc_as_upsert': True}, request_timeout=30)

    def add_transaction(self, tx):
        "Add a transaction. Do nothing if the block already exists"

        the_index = "btc-transactions"
        try:
            #self.es.get(index=the_index, doc_type="doc", id=tx['hash'])
            self.es.get(index=the_index, id=tx['hash'])
            # It exists if this returns, let's skip it
        except NotFoundError:
            # We need to add this transaction
            #self.es.update(id=tx['hash'], index=the_index, doc_type='doc', body={'doc' :tx, 'doc_as_upsert': True}, request_timeout=30)
            self.es.update(id=tx['hash'], index=the_index, body={'doc' :tx, 'doc_as_upsert': True}, request_timeout=30)

    def add_price(self, date, price):
        "Add the price for a given timestamp"
        price_data = { 'time': date, 'price': price }
        #self.es.update(id=date, index="btc-price-date", doc_type='_doc', body={'doc' :price_data, 'doc_as_upsert': True}, request_timeout=30)
        self.es.update(id=date, index="btc-price-date", body={'doc' :price_data, 'doc_as_upsert': True}, request_timeout=30)

    def add_opreturn_files(self, data):
        errors = []

        for ok, item in elasticsearch.helpers.streaming_bulk(self.es, data, max_retries=2):
            if not ok:
                errors.append(item)

        return errors

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

    def __init__(self, url, timeout=90):
        self.rpc = AuthServiceProxy(url, timeout=timeout)


    def get_block(self, i):
        block = self.rpc.getblockhash(i)
        block_data = self.rpc.getblock(block)
        block_data['transactions'] = len(block_data['tx'])
        # Elasticsearch struggles with these as integers
        #block_data['chainwork_int'] = int(block_data['chainwork'], 16)
        block_data['difficulty'] = int(block_data['difficulty'])
        del(block_data['tx'])

        # Figure out how many coins moved
        value = 0
        txs = self.get_block_transactions(i)

        # This is the data we need for value
        # txs[0]['vout'][0]['value']
        for tx in txs:
            for vout in tx['vout']:
                if vout['scriptPubKey']['type'] == 'nonstandard':
                    pass
                else:
                    value = value + vout['value']

        block_data['value'] = value

        return block_data

    def get_transaction(self, tx):
        rtx = self.rpc.getrawtransaction(tx)
        dtx = self.rpc.decoderawtransaction(rtx)

        return dtx

    def get_transactions(self, txs):
        rtx = self.rpc.batch_([["getrawtransaction", t] for t in txs])
        dtx = self.rpc.batch_([["decoderawtransaction", t] for t in rtx])

        return dtx

    def get_block_transactions(self, block):

        # The genesis block is special
        if block == 0:
            return []

        blockhash = self.rpc.getblockhash(block)
        block_data = self.rpc.getblock(blockhash)

        transactions = []

        rtx = self.rpc.batch_([["getrawtransaction", t] for t in block_data['tx']])
        dtx = self.rpc.batch_([["decoderawtransaction", t] for t in rtx])

        for tx in dtx:
            tx['height'] = block_data['height']
            tx['block'] = block_data['hash']
            tx['time'] = block_data['time']

            # We can't use this data, let's get rid of it
            for i in tx['vin']:
                if 'scriptSig' in i:
                    del(i['scriptSig'])
            for i in tx['vout']:
                if 'hex' in i['scriptPubKey']:
                    del(i['scriptPubKey']['hex'])
                if 'asm' in i['scriptPubKey']:
                    del(i['scriptPubKey']['asm'])

            transactions.append(tx)

        return transactions

    def get_block_transactions_bulk(self, block):
        "Return an iterable object for bulk transactions"

        transactions = self.get_block_transactions(block)
        tx = Transactions()
        for i in transactions:
            tx.add_transaction(i)

        return tx

    def get_blocks_bulk(self, blocks):

        rbh = self.rpc.batch_([["getblockhash", t] for t in blocks])

        dbh = self.rpc.batch_([["get_block", t] for t in rbh])

        output = []
        for block_data in dbh:
            block_data['transactions'] = len(block_data['tx'])
            block_data['chainwork_int'] = int(block_data['chainwork'], 16)
            del(block_data['tx'])
            output.append(block_data)

        return output

    def get_max_block(self):
        return self.rpc.getblockcount()

class Transactions:

    def __init__(self):
        self.transactions = []
        self.current = -1

    def add_transaction(self, tx):
        temp = {
                    #'_type': 'doc',
                    '_op_type': 'update',
                    '_index': "btc-transactions",
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

        if len(self.transactions) > 200:
            try:
                self.es_handle.add_bulk_tx(self)
            except TransportError:
                import pdb; pdb.set_trace
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
