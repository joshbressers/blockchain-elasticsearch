#!/usr/bin/env python

import sys
from datetime import datetime
import json
from urllib.request import Request, urlopen

from esbtc import ElasticsearchBTC

es = ElasticsearchBTC()

if len(sys.argv) > 1 and sys.argv[1] == "all":
    q = Request("https://api.coindesk.com/v1/bpi/historical/close.json?start=2010-07-17&end=%s" % datetime.today().strftime("%Y-%m-%d"))
else:
    q = Request("https://api.coindesk.com/v1/bpi/historical/close.json")

q.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)')
data = json.loads(urlopen(q).read().decode('ascii'))

total = len(data['bpi'])
current = 0

for i in data['bpi']:
    date = datetime.strptime(i, '%Y-%m-%d')
    epoch = int(date.timestamp())
    es.add_price(epoch, data['bpi'][i])
    print("adding %d/%d" % (current+1, total))
    current = current + 1
