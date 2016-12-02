set hive.strict.checks.cartesian.product=false;
set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.join.emit.interval=2;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

CREATE TABLE aux1 (key INT, value INT, col_1 STRING);
INSERT INTO aux1 VALUES (NULL, NULL, 'None'), (98, NULL, 'None'),
    (99, 0, 'Alice'), (99, 2, 'Mat'), (100, 1, 'Bob'), (101, 2, 'Car');

CREATE TABLE test1 (key INT, value INT, col_1 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT OVERWRITE TABLE test1
SELECT * FROM aux1;

CREATE TABLE aux2 (key INT, value INT, col_2 STRING);
INSERT INTO aux2 VALUES (102, 2, 'Del'), (103, 2, 'Ema'),
    (104, 3, 'Fli'), (105, NULL, 'None');

CREATE TABLE test2 (key INT, value INT, col_2 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT OVERWRITE TABLE test2
SELECT * FROM aux2;


-- Basic outer join
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value);

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value);

-- Conjunction with pred on multiple inputs and single inputs (left outer join)
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  AND test1.key between 100 and 102
  AND test2.key between 100 and 102);

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  AND test1.key between 100 and 102
  AND test2.key between 100 and 102);

-- Conjunction with pred on single inputs (left outer join)
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.key between 100 and 102
  AND test2.key between 100 and 102);

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.key between 100 and 102
  AND test2.key between 100 and 102);

-- Conjunction with pred on multiple inputs and none (left outer join)
EXPLAIN
SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value AND true);

SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value AND true);

-- Condition on one input (left outer join)
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.key between 100 and 102);

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.key between 100 and 102);

-- Disjunction with pred on multiple inputs and single inputs (left outer join)
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102);

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (left outer join)
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102);

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (left outer join)
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  OR test2.key between 100 and 102);

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  OR test2.key between 100 and 102);

-- Keys plus residual (left outer join)
EXPLAIN
SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  AND (test1.key between 100 and 102
    OR test2.key between 100 and 102));

SELECT *
FROM test1 LEFT OUTER JOIN test2
ON (test1.value=test2.value
  AND (test1.key between 100 and 102
    OR test2.key between 100 and 102));

-- Disjunction with pred on multiple inputs and single inputs (right outer join)
EXPLAIN
SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102);

SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (right outer join)
EXPLAIN
SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102);

SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (right outer join)
EXPLAIN
SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  OR test2.key between 100 and 102);

SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  OR test2.key between 100 and 102);

-- Keys plus residual (right outer join)
EXPLAIN
SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  AND (test1.key between 100 and 102
    OR test2.key between 100 and 102));

SELECT *
FROM test1 RIGHT OUTER JOIN test2
ON (test1.value=test2.value
  AND (test1.key between 100 and 102
    OR test2.key between 100 and 102));

-- Disjunction with pred on multiple inputs and single inputs (full outer join)
EXPLAIN
SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102);

SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102);

-- Disjunction with pred on multiple inputs and left input (full outer join)
EXPLAIN
SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102);

SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102);

-- Disjunction with pred on multiple inputs and right input (full outer join)
EXPLAIN
SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  OR test2.key between 100 and 102);

SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  OR test2.key between 100 and 102);

-- Keys plus residual (full outer join)
EXPLAIN
SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  AND (test1.key between 100 and 102
    OR test2.key between 100 and 102));

SELECT *
FROM test1 FULL OUTER JOIN test2
ON (test1.value=test2.value
  AND (test1.key between 100 and 102
    OR test2.key between 100 and 102));

