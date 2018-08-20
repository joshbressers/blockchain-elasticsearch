#!/usr/bin/env python

import sys
import os

files = os.listdir(sys.argv[1])

good_files = open("interesting.txt", "w")

for i in files:
    if i == '.' or i == '..':
        continue

    filename = os.path.join(sys.argv[1], i)
    fh = open(filename, 'rb')

    print("==========================================================================")
    print(filename)
    print("--------------------------------------------------------------------------")
    print(str(fh.read(), 'utf-8', errors='replace'))
    print("==========================================================================")
    the_input = input("Action \(n\)ext \(t\)ag:")
    if the_input == 'n':
        continue
    elif the_input == 't':
        good_files.write(filename)
        good_files.write("\n")
    fh.close()

good_files.close()
