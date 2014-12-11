set hive.fetch.task.conversion=more;
set hive.optimize.constant.propagation=true;

set mapred.reduce.tasks=4;

EXPLAIN
SELECT src1.key, src1.key + 1, src2.value
       FROM src src1 join src src2 ON src1.key = src2.key AND src1.key = 100;

SELECT src1.key, src1.key + 1, src2.value
       FROM src src1 join src src2 ON src1.key = src2.key AND src1.key = 100;
