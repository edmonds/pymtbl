#!/usr/bin/env python

import locale
import random
import sys
import time

import mtbl

report_interval = 10000

def main(fname, num_keys, num_iters):
    reader = mtbl.reader(fname)

    a = time.time()
    last = a
    num_found = 0
    count = 0
    while count < num_iters:
        key = '%010d' % random.randint(0, num_keys)
        if reader.has_key(key):
            val = reader[key]
            num_found += 1
        count += 1
        if (count % report_interval) == 0:
            b = time.time()
            last_secs = b - last
            last = b
            sys.stderr.write('%s lookups, %s keys found in %s seconds, %s lookups/second\n' % (
                locale.format('%d', count, grouping=True),
                locale.format('%d', num_found, grouping=True),
                locale.format('%f', last_secs, grouping=True),
                locale.format('%d', report_interval / last_secs, grouping=True)
                )
            )
    b = time.time()
    total_secs = b - a
    sys.stderr.write('%s total lookups, %s keys found in %s seconds, %s lookups/second\n' % (
        locale.format('%d', count, grouping=True),
        locale.format('%d', num_found, grouping=True),
        locale.format('%f', total_secs, grouping=True),
        locale.format('%d', count / total_secs, grouping=True)
        )
    )

def usage():
    sys.stderr.write('Usage: %s <MTBL FILENAME> <NUMBER OF KEYS> <NUMBER OF ITERATIONS>\n' %
        sys.argv[0])
    sys.exit(1)

if __name__ == '__main__':
    locale.setlocale(locale.LC_ALL, '')
    if len(sys.argv) != 4:
        usage()
    try:
        fname = sys.argv[1]
        num_keys = int(sys.argv[2])
        num_iters = int(sys.argv[3])
    except:
        usage()
    main(fname, num_keys, num_iters)
