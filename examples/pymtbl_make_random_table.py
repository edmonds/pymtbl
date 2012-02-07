#!/usr/bin/env python

import locale
import random
import string
import sys
import time

import mtbl

report_interval = 10000000
megabyte = 1048576.0

def main(fname, num_keys):
    writer = mtbl.writer(fname, compression=mtbl.COMPRESSION_SNAPPY)

    a = time.time()
    last = a
    total_bytes = 0
    count = 0
    while count < num_keys:
        count += 1
        if random.random() >= 0.5:
            key = '%010d' % count
            val = random.choice(string.ascii_lowercase) * random.randint(1, 50)
            writer[key] = val
            total_bytes += len(key) + len(val)
        if (count % report_interval) == 0:
            b = time.time()
            last_secs = b - last
            last = b
            sys.stderr.write('wrote %s entries (%s MB) in %s seconds, %s entries/second\n' % (
                locale.format('%d', count, grouping=True),
                locale.format('%d', total_bytes / megabyte, grouping=True),
                locale.format('%f', last_secs, grouping=True),
                locale.format('%d', report_interval / last_secs, grouping=True)
                )
            )
    b = time.time()
    total_secs = b - a
    sys.stderr.write('wrote %s total entries (%s MB) in %s seconds, %s entries/second\n' % (
        locale.format('%d', count, grouping=True),
        locale.format('%d', total_bytes / megabyte, grouping=True),
        locale.format('%f', total_secs, grouping=True),
        locale.format('%d', count / total_secs, grouping=True)
        )
    )

def usage():
    sys.stderr.write('Usage: %s <MTBL FILENAME> <MAX NUMBER OF KEYS>\n' % sys.argv[0])
    sys.exit(1)

if __name__ == '__main__':
    locale.setlocale(locale.LC_ALL, '')
    if len(sys.argv) != 3:
        usage()
    try:
        fname = sys.argv[1]
        num_keys = int(sys.argv[2])
    except:
        usage()
    main(fname, num_keys)
