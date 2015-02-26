#!/usr/bin/env python
import sys

for line in sys.stdin:

    line = line.strip()
    title, page = line.split('\t', 1)
    print '%s\t%s'   %   ( title, page )
