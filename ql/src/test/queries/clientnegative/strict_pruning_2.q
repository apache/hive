set hive.strict.checks.bucketing=false; 

reset hive.mapred.mode;
set hive.strict.checks.large.query=true;

EXPLAIN
SELECT count(1) FROM srcPART;

SELECT count(1) FROM srcPART;
