#!/usr/bin/env python

import sys
import os
import base64
import subprocess

from esbtc import ElasticsearchBTC

def ascii_only(in_bytes):
    # We can only index ascii easily, yank out only the ascii
    # XXX: Fix this someday
    out_bytes = b''
    for byte in in_bytes:
        if byte > 31 and byte < 127:
            # The byte variable ends up looking like an int, we have to
            # turn it back into a byte
            out_bytes = out_bytes + bytes([byte])
        else:
            out_bytes = out_bytes + b' '

    return out_bytes

def decode_string(b64string):
    # I bet this isn't perfect, but it mostly works. We can fix it later
    # The format we will see is 21 characters, then ==, possibly repeated.
    # We can remove the ==, but we need one at the very end, then decode

    newstr = b64string.replace(b'=', b'')
    newstr = newstr + b'=='

    try:
        decoded = base64.b64decode(newstr)
    except:
        decoded = b''

    return decoded

def main():

    es = ElasticsearchBTC()

    files = []

    search_dirs = []
    if len(sys.argv) > 1:
        search_dirs.append(sys.argv[1])
    else:
        search_dirs = os.listdir('sorted')

    search_total = len(search_dirs)
    search_current = 0
    for f in search_dirs:

        if f == '.' or f == '..':
            continue

        output = os.listdir(os.path.join('sorted', f))
        output.sort()
        to_index = []

        print("%s %d/%d" % (f, search_current, search_total))
        search_current = search_current + 1

        for i in output:
            if i == '.' or i == '..':
                continue

            base64decode = False

            tx_id = i
            filename = os.path.join('sorted', f, i)
            fh = open(filename, 'rb')

            byte_count = 0
            ascii_count = 0
            equals = 0

            binary_data = fh.read()
            byte_count = len(binary_data)
            file_data = ascii_only(binary_data)

            # The base64 encoded stuff has 21 characters then an ==
            if file_data[22:24] == b'==':

                new_data = decode_string(file_data)
                if new_data:
                    file_data = new_data
                    filename = filename + ".decoded"
                    decodefh = open(filename, 'wb')
                    decodefh.write(file_data)
                    decodefh.close()

            #file_data = str(file_data, 'ascii', errors='replace')
            output_data = file_data.decode(encoding="ascii", errors="replace")

            if len(output_data) > 32766:
                # This needs to be fixed, some of these massive files do weird
                # things to the decoder
                output_data = output_data[0:32765]
            fh.close()

            filetype = subprocess.check_output(['file', '-b', filename])
            filetype = str(filetype.rstrip(), 'ascii', errors='replace')

            the_data = {"tx": tx_id,
                        "type": filetype,
                        "size": byte_count,
                        "block": f,
                        "data": output_data
                       }
            temp = {    '_type': 'doc',
                        '_op_type': 'update',
                        '_index': "btc-opreturn-file",
                        '_id': tx_id,
                        'doc_as_upsert': True,
                        'doc': the_data
                    }

            to_index.append(temp)

        es.add_opreturn_files(to_index)

if __name__ == "__main__":
    # execute only if run as a script
    main()
