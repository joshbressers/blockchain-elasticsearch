#!/usr/bin/env python

import sys
from datetime import datetime
import json
from urllib.request import Request, urlopen

from esbtc import ElasticsearchBTC

es = ElasticsearchBTC()

q = Request("https://api.coindesk.com/v1/bpi/historical/close.json")
q.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)')
data = json.loads(urlopen(q).read().decode('ascii'))

for i in data['bpi']:
    date = datetime.strptime(i, '%Y-%m-%d')
    epoch = date.timestamp()
    es.add_price(epoch, data['bpi'][i])
