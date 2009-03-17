set hive.partition.pruning=strict;

EXPLAIN
SELECT count(1) FROM srcPART x where x.ds = 2009-08-01;

SELECT count(1) FROM srcPART where x.ds = 2009-08-01;
