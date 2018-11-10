set hive.cli.print.header=true;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=true;
set hive.strict.checks.cartesian.product=false;
set hive.join.emit.interval=2;

-- SORT_QUERY_RESULTS

CREATE TABLE test1_n14 (key INT, value INT, col_1 STRING);
INSERT INTO test1_n14 VALUES (NULL, NULL, 'None'), (98, NULL, 'None'),
    (99, 0, 'Alice'), (99, 2, 'Mat'), (100, 1, 'Bob'), (101, 2, 'Car');

CREATE TABLE test2_n9 (key INT, value INT, col_2 STRING);
INSERT INTO test2_n9 VALUES (102, 2, 'Del'), (103, 2, 'Ema'),
    (104, 3, 'Fli'), (105, NULL, 'None');


-- Basic outer join
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value);

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value);

-- Conjunction with pred on multiple inputs and single inputs (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND test1_n14.key between 100 and 102
  AND test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND test1_n14.key between 100 and 102
  AND test2_n9.key between 100 and 102);

-- Conjunction with pred on single inputs (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.key between 100 and 102
  AND test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.key between 100 and 102
  AND test2_n9.key between 100 and 102);

-- Conjunction with pred on multiple inputs and none (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value AND true);

SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value AND true);

-- Condition on one input (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.key between 100 and 102);

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.key between 100 and 102);

-- Disjunction with pred on multiple inputs and single inputs (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102
  OR test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102
  OR test2_n9.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102);

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test2_n9.key between 100 and 102);

-- Keys plus residual (left outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND (test1_n14.key between 100 and 102
    OR test2_n9.key between 100 and 102));

SELECT *
FROM test1_n14 LEFT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND (test1_n14.key between 100 and 102
    OR test2_n9.key between 100 and 102));

-- Disjunction with pred on multiple inputs and single inputs (right outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102
  OR test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102
  OR test2_n9.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (right outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102);

SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (right outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test2_n9.key between 100 and 102);

-- Keys plus residual (right outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND (test1_n14.key between 100 and 102
    OR test2_n9.key between 100 and 102));

SELECT *
FROM test1_n14 RIGHT OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND (test1_n14.key between 100 and 102
    OR test2_n9.key between 100 and 102));

-- Disjunction with pred on multiple inputs and single inputs (full outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102
  OR test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102
  OR test2_n9.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (full outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102);

SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test1_n14.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (full outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test2_n9.key between 100 and 102);

SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  OR test2_n9.key between 100 and 102);

-- Keys plus residual (full outer join)
EXPLAIN VECTORIZATION OPERATOR
SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND (test1_n14.key between 100 and 102
    OR test2_n9.key between 100 and 102));

SELECT *
FROM test1_n14 FULL OUTER JOIN test2_n9
ON (test1_n14.value=test2_n9.value
  AND (test1_n14.key between 100 and 102
    OR test2_n9.key between 100 and 102));
