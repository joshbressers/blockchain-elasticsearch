
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionTimeout
import elasticsearch
import elasticsearch.helpers
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

class ElasticsearchBTC:
    "Class for querying the Elasticsearch BTC instance"

    def __init__(self, url):

        self.url = url
        self.es = Elasticsearch([self.url])
        self.size = None

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

    def get_block_transactions(self, block):
        blockhash = self.rpc.getblockhash(block)
        block_data = self.rpc.getblock(blockhash)

        transactions = []

        for tx in block_data['tx']:
            # Load the transaction
            rtx = self.rpc.getrawtransaction(tx)
            dtx  = self.rpc.decoderawtransaction(rtx)
            dtx['height'] = block_data['height']
            dtx['block'] = block_data['hash']
            dtx['time'] = block_data['time']
            transactions.append(dtx)

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
