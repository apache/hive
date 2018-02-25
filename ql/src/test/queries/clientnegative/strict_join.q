set hive.strict.checks.bucketing=false; 

set hive.mapred.mode=strict;

SELECT *  FROM src src1 JOIN src src2;

reset hive.mapred.mode;
set hive.strict.checks.cartesian.product=true;

SELECT *  FROM src src1 JOIN src src2;
