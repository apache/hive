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
set hive.join.inner.residual=true;

CREATE TABLE aux1_n0 (key INT, value INT, col_1 STRING);
INSERT INTO aux1_n0 VALUES (NULL, NULL, 'None'), (98, NULL, 'None'),
    (99, 0, 'Alice'), (99, 2, 'Mat'), (100, 1, 'Bob'), (101, 2, 'Car');

CREATE TABLE test1_n8 (key INT, value INT, col_1 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT OVERWRITE TABLE test1_n8
SELECT * FROM aux1_n0;

CREATE TABLE aux2_n0 (key INT, value INT, col_2 STRING);
INSERT INTO aux2_n0 VALUES (102, 2, 'Del'), (103, 2, 'Ema'),
    (104, 3, 'Fli'), (105, NULL, 'None');

CREATE TABLE test2_n5 (key INT, value INT, col_2 STRING) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
INSERT OVERWRITE TABLE test2_n5
SELECT * FROM aux2_n0;

-- Conjunction with pred on multiple inputs and single inputs
EXPLAIN
SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.value=test2_n5.value
  AND test1_n8.key between 100 and 102
  AND test2_n5.key between 100 and 102)
LIMIT 10;

SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.value=test2_n5.value
  AND test1_n8.key between 100 and 102
  AND test2_n5.key between 100 and 102)
LIMIT 10;

-- Conjunction with pred on multiple inputs and none
EXPLAIN
SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.value=test2_n5.value AND true)
LIMIT 10;

SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.value=test2_n5.value AND true)
LIMIT 10;

-- Conjunction with pred on single inputs and none
EXPLAIN
SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.key between 100 and 102
  AND test2_n5.key between 100 and 102
  AND true)
LIMIT 10;

SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.key between 100 and 102
  AND test2_n5.key between 100 and 102
  AND true)
LIMIT 10;

-- Disjunction with pred on multiple inputs and single inputs
EXPLAIN
SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.value=test2_n5.value
  OR test1_n8.key between 100 and 102
  OR test2_n5.key between 100 and 102)
LIMIT 10;

SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.value=test2_n5.value
  OR test1_n8.key between 100 and 102
  OR test2_n5.key between 100 and 102)
LIMIT 10;

-- Conjunction with multiple inputs on one side
EXPLAIN
SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.key+test2_n5.key >= 100
  AND test1_n8.key+test2_n5.key <= 102)
LIMIT 10;

SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.key+test2_n5.key >= 100
  AND test1_n8.key+test2_n5.key <= 102)
LIMIT 10;

-- Disjunction with multiple inputs on one side
EXPLAIN
SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.key+test2_n5.key >= 100
  OR test1_n8.key+test2_n5.key <= 102)
LIMIT 10;

SELECT *
FROM test1_n8 JOIN test2_n5
ON (test1_n8.key+test2_n5.key >= 100
  OR test1_n8.key+test2_n5.key <= 102)
LIMIT 10;

-- Function with multiple inputs on one side
EXPLAIN
SELECT *
FROM test1_n8 JOIN test2_n5
ON ((test1_n8.key,test2_n5.key) IN ((100,100),(101,101),(102,102)))
LIMIT 10;

SELECT *
FROM test1_n8 JOIN test2_n5
ON ((test1_n8.key,test2_n5.key) IN ((100,100),(101,101),(102,102)))
LIMIT 10;

-- Chained 1
EXPLAIN
SELECT *
FROM test2_n5
JOIN test1_n8 a ON (a.key+test2_n5.key >= 100)
LEFT OUTER JOIN test1_n8 b ON (b.value = test2_n5.value)
LIMIT 10;

SELECT *
FROM test2_n5
JOIN test1_n8 a ON (a.key+test2_n5.key >= 100)
LEFT OUTER JOIN test1_n8 b ON (b.value = test2_n5.value)
LIMIT 10;

-- Chained 2
EXPLAIN
SELECT *
FROM test2_n5
LEFT OUTER JOIN test1_n8 a ON (a.value = test2_n5.value)
JOIN test1_n8 b ON (b.key+test2_n5.key<= 102)
LIMIT 10;

SELECT *
FROM test2_n5
LEFT OUTER JOIN test1_n8 a ON (a.value = test2_n5.value)
JOIN test1_n8 b ON (b.key+test2_n5.key<= 102)
LIMIT 10;

-- Chained 3
EXPLAIN
SELECT *
FROM test2_n5
JOIN test1_n8 a ON (a.key+test2_n5.key >= 100)
RIGHT OUTER JOIN test1_n8 b ON (b.value = test2_n5.value)
LIMIT 10;

SELECT *
FROM test2_n5
JOIN test1_n8 a ON (a.key+test2_n5.key >= 100)
RIGHT OUTER JOIN test1_n8 b ON (b.value = test2_n5.value)
LIMIT 10;

-- Chained 4
EXPLAIN
SELECT *
FROM test2_n5
RIGHT OUTER JOIN test1_n8 a ON (a.value = test2_n5.value)
JOIN test1_n8 b ON (b.key+test2_n5.key<= 102)
LIMIT 10;

SELECT *
FROM test2_n5
RIGHT OUTER JOIN test1_n8 a ON (a.value = test2_n5.value)
JOIN test1_n8 b ON (b.key+test2_n5.key<= 102)
LIMIT 10;

-- Chained 5
EXPLAIN
SELECT *
FROM test2_n5
JOIN test1_n8 a ON (a.key+test2_n5.key >= 100)
FULL OUTER JOIN test1_n8 b ON (b.value = test2_n5.value)
LIMIT 10;

SELECT *
FROM test2_n5
JOIN test1_n8 a ON (a.key+test2_n5.key >= 100)
FULL OUTER JOIN test1_n8 b ON (b.value = test2_n5.value)
LIMIT 10;

-- Chained 6
EXPLAIN
SELECT *
FROM test2_n5
FULL OUTER JOIN test1_n8 a ON (a.value = test2_n5.value)
JOIN test1_n8 b ON (b.key+test2_n5.key<= 102)
LIMIT 10;

SELECT *
FROM test2_n5
FULL OUTER JOIN test1_n8 a ON (a.value = test2_n5.value)
JOIN test1_n8 b ON (b.key+test2_n5.key<= 102)
LIMIT 10;
