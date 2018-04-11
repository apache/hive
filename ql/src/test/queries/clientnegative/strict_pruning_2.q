reset hive.mapred.mode;
set hive.strict.checks.no.partition.filter=true;

EXPLAIN
SELECT count(1) FROM srcPART;

SELECT count(1) FROM srcPART;
