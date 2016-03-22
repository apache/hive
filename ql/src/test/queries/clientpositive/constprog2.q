set hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=more;
set hive.optimize.constant.propagation=true;

EXPLAIN
SELECT src1.key, src1.key + 1, src2.value
       FROM srcbucket src1 join srcbucket src2 ON src1.key = src2.key AND src1.key = 86;

EXPLAIN
SELECT src1.key, src1.key + 1, src2.value
       FROM srcbucket src1 join srcbucket src2 ON src1.key = src2.key AND cast(src1.key as double) = 86;

