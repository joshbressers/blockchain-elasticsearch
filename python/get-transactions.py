#!/usr/bin/env python

import sys
import socket
import time
from queue import Queue
from queue import Empty
from threading import Thread

from esbtc import ElasticsearchBTC
from esbtc import DaemonBTC

def block_worker():
    while True:
        try:
            i = tx_q.get(timeout=10)
            print("block %d/%d"%(i['height'], height))
            es.add_transaction(i)
        except KeyboardInterrupt as e:
            sys.exit(1)

        except Empty:
            # We're done here
            return

        except socket.timeout:
            # Something went wrong, put it back in the queue
            tx_q.put(i)



rpc = DaemonBTC("http://test:test@127.0.0.1:8332")
es = ElasticsearchBTC()

tx_q = Queue(maxsize=1000)
count_q = Queue()

height = rpc.get_max_block()

for i in range(10):
    t = Thread(target=block_worker)
    t.daemon = True
    t.start()

# We have to skip block 0
size = 1
if len(sys.argv) > 1:
    size = int(sys.argv[1])

for i in range(size, height + 1):
    count_q.put(i)

# Don't try to thread this, bad things happen if we hit the bitcoin server
# with more than one client
while not count_q.empty():
    try:
        i = count_q.get()

        for tx in rpc.get_block_transactions(i):
            tx_q.put(tx)
    except socket.timeout:
        # Put the count back in the queue
        count_q.put(i)
    except:
        # We need to catch certain exceptions
        # probably
        raise

while not tx_q.empty():
    time.sleep(2)

