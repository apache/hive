SET hive.vectorized.execution.enabled=false;
set hive.auto.convert.join=true;
set hive.strict.checks.cartesian.product=false;
set hive.join.emit.interval=2;

CREATE TABLE test1_n4 (key INT, value INT, col_1 STRING);
INSERT INTO test1_n4 VALUES (NULL, NULL, 'None'), (98, NULL, 'None'),
    (99, 0, 'Alice'), (99, 2, 'Mat'), (100, 1, 'Bob'), (101, 2, 'Car');

CREATE TABLE test2_n2 (key INT, value INT, col_2 STRING);
INSERT INTO test2_n2 VALUES (102, 2, 'Del'), (103, 2, 'Ema'),
    (104, 3, 'Fli'), (105, NULL, 'None');


-- Basic outer join
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value);

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value);

-- Conjunction with pred on multiple inputs and single inputs (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND test1_n4.key between 100 and 102
  AND test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND test1_n4.key between 100 and 102
  AND test2_n2.key between 100 and 102);

-- Conjunction with pred on single inputs (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.key between 100 and 102
  AND test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.key between 100 and 102
  AND test2_n2.key between 100 and 102);

-- Conjunction with pred on multiple inputs and none (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value AND true);

SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value AND true);

-- Condition on one input (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.key between 100 and 102);

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.key between 100 and 102);

-- Disjunction with pred on multiple inputs and single inputs (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102
  OR test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102
  OR test2_n2.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102);

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test2_n2.key between 100 and 102);

-- Keys plus residual (left outer join)
EXPLAIN
SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND (test1_n4.key between 100 and 102
    OR test2_n2.key between 100 and 102));

SELECT *
FROM test1_n4 LEFT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND (test1_n4.key between 100 and 102
    OR test2_n2.key between 100 and 102));

-- Disjunction with pred on multiple inputs and single inputs (right outer join)
EXPLAIN
SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102
  OR test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102
  OR test2_n2.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (right outer join)
EXPLAIN
SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102);

SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (right outer join)
EXPLAIN
SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test2_n2.key between 100 and 102);

-- Keys plus residual (right outer join)
EXPLAIN
SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND (test1_n4.key between 100 and 102
    OR test2_n2.key between 100 and 102));

SELECT *
FROM test1_n4 RIGHT OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND (test1_n4.key between 100 and 102
    OR test2_n2.key between 100 and 102));

-- Disjunction with pred on multiple inputs and single inputs (full outer join)
EXPLAIN
SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102
  OR test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102
  OR test2_n2.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (full outer join)
EXPLAIN
SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102);

SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test1_n4.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (full outer join)
EXPLAIN
SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test2_n2.key between 100 and 102);

SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  OR test2_n2.key between 100 and 102);

-- Keys plus residual (full outer join)
EXPLAIN
SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND (test1_n4.key between 100 and 102
    OR test2_n2.key between 100 and 102));

SELECT *
FROM test1_n4 FULL OUTER JOIN test2_n2
ON (test1_n4.value=test2_n2.value
  AND (test1_n4.key between 100 and 102
    OR test2_n2.key between 100 and 102));

-- Mixed ( FOJ (ROJ, LOJ) ) 
EXPLAIN
SELECT *
FROM (
  SELECT test1_n4.key AS key1, test1_n4.value AS value1, test1_n4.col_1 AS col_1,
         test2_n2.key AS key2, test2_n2.value AS value2, test2_n2.col_2 AS col_2
  FROM test1_n4 RIGHT OUTER JOIN test2_n2
  ON (test1_n4.value=test2_n2.value
    AND (test1_n4.key between 100 and 102
      OR test2_n2.key between 100 and 102))
  ) sq1
FULL OUTER JOIN (
  SELECT test1_n4.key AS key3, test1_n4.value AS value3, test1_n4.col_1 AS col_3,
         test2_n2.key AS key4, test2_n2.value AS value4, test2_n2.col_2 AS col_4
  FROM test1_n4 LEFT OUTER JOIN test2_n2
  ON (test1_n4.value=test2_n2.value
    AND (test1_n4.key between 100 and 102
      OR test2_n2.key between 100 and 102))
  ) sq2
ON (sq1.value1 is null or sq2.value4 is null and sq2.value3 != sq1.value2);

SELECT *
FROM (
  SELECT test1_n4.key AS key1, test1_n4.value AS value1, test1_n4.col_1 AS col_1,
         test2_n2.key AS key2, test2_n2.value AS value2, test2_n2.col_2 AS col_2
  FROM test1_n4 RIGHT OUTER JOIN test2_n2
  ON (test1_n4.value=test2_n2.value
    AND (test1_n4.key between 100 and 102
      OR test2_n2.key between 100 and 102))
  ) sq1
FULL OUTER JOIN (
  SELECT test1_n4.key AS key3, test1_n4.value AS value3, test1_n4.col_1 AS col_3,
         test2_n2.key AS key4, test2_n2.value AS value4, test2_n2.col_2 AS col_4
  FROM test1_n4 LEFT OUTER JOIN test2_n2
  ON (test1_n4.value=test2_n2.value
    AND (test1_n4.key between 100 and 102
      OR test2_n2.key between 100 and 102))
  ) sq2
ON (sq1.value1 is null or sq2.value4 is null and sq2.value3 != sq1.value2);
