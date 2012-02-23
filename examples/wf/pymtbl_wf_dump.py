#!/usr/bin/env python

import string
import sys

import mtbl

def main(mtbl_fname):
    reader = mtbl.reader(mtbl_fname)
    for k, v in reader.items():
        word = k
        count = mtbl.varint_decode(v)
        print '%s\t%s' % (count, word)

if __name__ == '__main__':
    if not len(sys.argv) == 2:
        sys.stderr.write('Usage: %s <MTBL FILE>\n' % sys.argv[0])
        sys.exit(1)
    main(sys.argv[1])
