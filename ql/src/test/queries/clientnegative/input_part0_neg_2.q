set hive.strict.checks.bucketing=false; 

reset hive.mapred.mode;
set hive.strict.checks.no.partition.filter=true;

SELECT x.* FROM SRCPART x WHERE key = '2008-04-08';
