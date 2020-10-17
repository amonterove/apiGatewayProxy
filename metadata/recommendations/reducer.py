#!/usr/bin/env python

from operator import itemgetter
import sys
from StringIO import StringIO

for line in sys.stdin:

    line = line.strip()

    print '%s' % (line)
