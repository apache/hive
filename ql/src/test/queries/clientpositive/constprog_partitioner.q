set hive.fetch.task.conversion=more;
set hive.optimize.constant.propagation=true;

set mapred.reduce.tasks=4;

EXPLAIN
SELECT src1.key, src1.key + 1, src2.value
       FROM src src1 join src src2 ON src1.key = src2.key AND src1.key = 100;

SELECT src1.key, src1.key + 1, src2.value
       FROM src src1 join src src2 ON src1.key = src2.key AND src1.key = 100;

EXPLAIN
SELECT l_partkey, l_suppkey
FROM lineitem li
WHERE li.l_linenumber = 1 AND
 li.l_orderkey IN (SELECT l_orderkey FROM lineitem WHERE l_shipmode = 'AIR' AND l_linenumber = li.l_linenumber)
;

SELECT l_partkey, l_suppkey
FROM lineitem li
WHERE li.l_linenumber = 1 AND
 li.l_orderkey IN (SELECT l_orderkey FROM lineitem WHERE l_shipmode = 'AIR' AND l_linenumber = li.l_linenumber)
;
