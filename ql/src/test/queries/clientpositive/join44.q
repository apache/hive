--! qt:dataset:src1
set hive.cbo.enable=false;

-- SORT_QUERY_RESULTS

CREATE TABLE mytable_n1(val1 INT, val2 INT, val3 INT);

EXPLAIN
SELECT *
FROM mytable_n1 src1, mytable_n1 src2
WHERE src1.val1=src2.val1
  AND src1.val2 between 2450816 and 2451500
  AND src2.val2 between 2450816 and 2451500;
