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

-- Conjunction with pred on multiple inputs and single inputs
EXPLAIN
SELECT *
FROM test1 JOIN test2
ON (test1.value=test2.value
  AND test1.key between 100 and 102
  AND test2.key between 100 and 102)
LIMIT 10;

SELECT *
FROM test1 JOIN test2
ON (test1.value=test2.value
  AND test1.key between 100 and 102
  AND test2.key between 100 and 102)
LIMIT 10;

-- Conjunction with pred on multiple inputs and none
EXPLAIN
SELECT *
FROM test1 JOIN test2
ON (test1.value=test2.value AND true)
LIMIT 10;

SELECT *
FROM test1 JOIN test2
ON (test1.value=test2.value AND true)
LIMIT 10;

-- Conjunction with pred on single inputs and none
EXPLAIN
SELECT *
FROM test1 JOIN test2
ON (test1.key between 100 and 102
  AND test2.key between 100 and 102
  AND true)
LIMIT 10;

SELECT *
FROM test1 JOIN test2
ON (test1.key between 100 and 102
  AND test2.key between 100 and 102
  AND true)
LIMIT 10;

-- Disjunction with pred on multiple inputs and single inputs
EXPLAIN
SELECT *
FROM test1 JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102)
LIMIT 10;

SELECT *
FROM test1 JOIN test2
ON (test1.value=test2.value
  OR test1.key between 100 and 102
  OR test2.key between 100 and 102)
LIMIT 10;

-- Conjunction with multiple inputs on one side
EXPLAIN
SELECT *
FROM test1 JOIN test2
ON (test1.key+test2.key >= 100
  AND test1.key+test2.key <= 102)
LIMIT 10;

SELECT *
FROM test1 JOIN test2
ON (test1.key+test2.key >= 100
  AND test1.key+test2.key <= 102)
LIMIT 10;

-- Disjunction with multiple inputs on one side
EXPLAIN
SELECT *
FROM test1 JOIN test2
ON (test1.key+test2.key >= 100
  OR test1.key+test2.key <= 102)
LIMIT 10;

SELECT *
FROM test1 JOIN test2
ON (test1.key+test2.key >= 100
  OR test1.key+test2.key <= 102)
LIMIT 10;

-- Function with multiple inputs on one side
EXPLAIN
SELECT *
FROM test1 JOIN test2
ON ((test1.key,test2.key) IN ((100,100),(101,101),(102,102)))
LIMIT 10;

SELECT *
FROM test1 JOIN test2
ON ((test1.key,test2.key) IN ((100,100),(101,101),(102,102)))
LIMIT 10;

-- Chained 1
EXPLAIN
SELECT *
FROM test2
JOIN test1 a ON (a.key+test2.key >= 100)
LEFT OUTER JOIN test1 b ON (b.value = test2.value)
LIMIT 10;

SELECT *
FROM test2
JOIN test1 a ON (a.key+test2.key >= 100)
LEFT OUTER JOIN test1 b ON (b.value = test2.value)
LIMIT 10;

-- Chained 2
EXPLAIN
SELECT *
FROM test2
LEFT OUTER JOIN test1 a ON (a.value = test2.value)
JOIN test1 b ON (b.key+test2.key<= 102)
LIMIT 10;

SELECT *
FROM test2
LEFT OUTER JOIN test1 a ON (a.value = test2.value)
JOIN test1 b ON (b.key+test2.key<= 102)
LIMIT 10;

-- Chained 3
EXPLAIN
SELECT *
FROM test2
JOIN test1 a ON (a.key+test2.key >= 100)
RIGHT OUTER JOIN test1 b ON (b.value = test2.value)
LIMIT 10;

SELECT *
FROM test2
JOIN test1 a ON (a.key+test2.key >= 100)
RIGHT OUTER JOIN test1 b ON (b.value = test2.value)
LIMIT 10;

-- Chained 4
EXPLAIN
SELECT *
FROM test2
RIGHT OUTER JOIN test1 a ON (a.value = test2.value)
JOIN test1 b ON (b.key+test2.key<= 102)
LIMIT 10;

SELECT *
FROM test2
RIGHT OUTER JOIN test1 a ON (a.value = test2.value)
JOIN test1 b ON (b.key+test2.key<= 102)
LIMIT 10;

-- Chained 5
EXPLAIN
SELECT *
FROM test2
JOIN test1 a ON (a.key+test2.key >= 100)
FULL OUTER JOIN test1 b ON (b.value = test2.value)
LIMIT 10;

SELECT *
FROM test2
JOIN test1 a ON (a.key+test2.key >= 100)
FULL OUTER JOIN test1 b ON (b.value = test2.value)
LIMIT 10;

-- Chained 6
EXPLAIN
SELECT *
FROM test2
FULL OUTER JOIN test1 a ON (a.value = test2.value)
JOIN test1 b ON (b.key+test2.key<= 102)
LIMIT 10;

SELECT *
FROM test2
FULL OUTER JOIN test1 a ON (a.value = test2.value)
JOIN test1 b ON (b.key+test2.key<= 102)
LIMIT 10;
