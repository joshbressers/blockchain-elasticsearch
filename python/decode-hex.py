#!/usr/bin/env python2

import sys

std_input = sys.stdin.readlines()

for i in std_input:
    the_string = i.rstrip()
    the_string = the_string.decode('hex')
    print("%s" % the_string)
