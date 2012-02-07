#!/usr/bin/env python

import sys

import mtbl

def merge_func(key, val0, val1):
    return val0 + ' ' + val1

def main(output_fname, input_fnames):
    merger = mtbl.merger(merge_func)
    writer = mtbl.writer(output_fname, compression=mtbl.COMPRESSION_SNAPPY)
    for fname in input_fnames:
        reader = mtbl.reader(fname)
        merger.add_reader(reader)
    for k, v in merger.iteritems():
        writer[k] = v
    writer.close()

def usage():
    sys.stderr.write('Usage: %s <OUTPUT> <INPUT> [<INPUT>...]\n' % sys.argv[0])
    sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
    try:
        output_fname = sys.argv[1]
        input_fnames = sys.argv[2:]
    except:
        usage()
    main(output_fname, input_fnames)
