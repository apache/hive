set hive.strict.checks.bucketing=false; 

set hive.mapred.mode=strict;

SELECT *  FROM src src1 JOIN src src2;
