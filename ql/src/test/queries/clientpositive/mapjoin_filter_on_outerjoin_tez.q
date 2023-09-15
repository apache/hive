--! qt:dataset:src1
--! qt:dataset:src
set hive.auto.convert.join = false;
set hive.merge.nway.joins=true;
-- SORT_QUERY_RESULTS

--HIVE-2101 mapjoin sometimes gives wrong results if there is a filter in the on condition

SELECT * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

explain
SELECT /*+ mapjoin(src1, src2) */ * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

SELECT /*+ mapjoin(src1, src2) */ * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

set hive.auto.convert.join = true;

explain
SELECT * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

SELECT * FROM src1
  RIGHT OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key > 10)
  JOIN src src3 ON (src2.key = src3.key AND src3.key < 300)
  SORT BY src1.key, src2.key, src3.key;

set hive.optimize.shared.work=true;
set hive.vectorized.execution.enabled=true;

explain
SELECT * FROM src1 FULL OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10);
SELECT * FROM src1 FULL OUTER JOIN src1 src2 ON (src1.key = src2.key AND src1.key < 10);

explain
SELECT * FROM src1 FULL OUTER JOIN src1 src2 ON (src1.key = src2.key AND src2.key < 10);
SELECT * FROM src1 FULL OUTER JOIN src1 src2 ON (src1.key = src2.key AND src2.key < 10);


-- Test FullOuterJoin with filters on both tables

set hive.optimize.dynamic.partition.hashjoin=true;
DROP TABLE IF EXISTS c;
CREATE TABLE c (key int, value int);
INSERT INTO c VALUES (1, 0), (2, 0);
DROP TABLE IF EXISTS d;
CREATE TABLE d (key int, value int);
INSERT INTO d VALUES (1, 1), (2, 1);

-- TOOD: Currently VectorMapJoin returns wrong result.
-- set hive.auto.convert.join=true;
-- set hive.vectorized.execution.enabled=true;

-- explain
-- SELECT * from c FULL OUTER JOIN d on c.key = d.key AND c.key > 0 AND d.key > 1;
-- SELECT * from c FULL OUTER JOIN d on c.key = d.key AND c.key > 0 AND d.key > 1;

set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=false;

explain
SELECT * from c FULL OUTER JOIN d on c.key = d.key AND c.key > 0 AND d.key > 1;
SELECT * from c FULL OUTER JOIN d on c.key = d.key AND c.key > 0 AND d.key > 1;

set hive.auto.convert.join=false;

explain
SELECT * from c FULL OUTER JOIN d on c.key = d.key AND c.key > 0 AND d.key > 1;
SELECT * from c FULL OUTER JOIN d on c.key = d.key AND c.key > 0 AND d.key > 1;
