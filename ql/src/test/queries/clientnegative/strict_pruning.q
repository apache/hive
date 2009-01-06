set hive.partition.pruning=strict;

EXPLAIN
SELECT count(1) FROM srcPART;

SELECT count(1) FROM srcPART;
