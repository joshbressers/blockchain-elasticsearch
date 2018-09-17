#!/usr/bin/env python

import sys
import os
import subprocess

from esbtc import ElasticsearchBTC

es = ElasticsearchBTC()

files = []
for f in ['a', 'b', 'c', 'd', 'e', 'f', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
    output = os.listdir(f)
    output.sort()
    to_index = []

    print(f)


    for i in output:
        if i == '.' or i == '..':
            continue

        tx_id = i
        filename = os.path.join(i[0], i)
        fh = open(filename, 'rb')

        byte_count = 0
        ascii_count = 0
        for byte in fh.read():
            byte_count = byte_count + 1
            if byte > 32 and byte < 126:
                ascii_count = ascii_count + 1

        file_data = ''
        if (byte_count > 0) and ((ascii_count/byte_count) > 0.8):
            fh.seek(0)
            file_data = str(fh.read(), 'ascii', errors='replace')

        if len(file_data) > 32766:
            file_data = ''
        fh.close()

        filetype = subprocess.check_output(['file', '-b', filename])
        filetype = str(filetype.rstrip(), 'ascii', errors='replace')

        the_data = {"tx": tx_id, "type": filetype, "size": byte_count, "data": file_data}
        temp = {    '_type': 'doc',
                    '_op_type': 'update',
                    '_index': "btc-opreturn-file",
                    '_id': tx_id,
                    'doc_as_upsert': True,
                    'doc': the_data
                }

        to_index.append(temp)

    es.add_opreturn_files(to_index)
