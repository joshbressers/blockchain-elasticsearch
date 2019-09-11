#!/bin/sh

curl -XPUT "$ESURL/_template/btc-blocks-template" -H 'Content-Type: application/json' -d @mapping-blocks.json
curl -XPUT "$ESURL/_template/btc-transactions-template" -H 'Content-Type: application/json' -d @mapping-transactions.json
curl -XPUT "$ESURL/_template/btc-price-template" -H 'Content-Type: application/json' -d @mapping-price-time.json
curl -XPUT "$ESURL/_template/btc-files-template" -H 'Content-Type: application/json' -d @mapping-files.json
