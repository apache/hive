import sys, re
import datetime
import os

table_name=None
if os.environ.has_key('hive_streaming_tablename'):
  table_name=os.environ['hive_streaming_tablename']

for line in sys.stdin:
  print line
  print >> sys.stderr, "dummy"
