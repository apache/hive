set hive.strict.checks.cartesian.product=false;

-- SORT_QUERY_RESULTS

CREATE TABLE mytable(val1 INT, val2 INT, val3 INT);

-- Outer join with complex pred: not supported
EXPLAIN
SELECT *
FROM mytable src1 LEFT OUTER JOIN mytable src2
ON (src1.val1+src2.val1>= 2450816
  AND src1.val1+src2.val1<= 2451500);

