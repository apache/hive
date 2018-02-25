set hive.strict.checks.bucketing=false; 

reset hive.mapred.mode;
set hive.strict.checks.cartesian.product=true;

SELECT *  FROM src src1 JOIN src src2;
