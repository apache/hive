--! qt:dataset:src
set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.convert.join.bucket.mapjoin.tez=true;

CREATE TABLE srcbucket_big(key1 string, key2 string, value string)
PARTITIONED BY SPEC(bucket(4, key1), bucket(2, key2)) STORED BY ICEBERG;
INSERT INTO srcbucket_big
SELECT key AS key1, value AS key2, value FROM src;

-- Using both key1 and key2
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key AND a.key2 = b.value
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key AND a.key2 = b.value
ORDER BY a.key1
LIMIT 20;

-- Using both key1 and key2, with a predicate
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key AND a.key2 = b.value
WHERE a.key1 NOT IN ('0', '100')
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key AND a.key2 = b.value
WHERE a.key1 NOT IN ('0', '100')
ORDER BY a.key1
LIMIT 20;

-- Using only key1
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key
ORDER BY a.key1
LIMIT 20;

-- Using only key1, with a predicate with key1
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key
WHERE a.key1 NOT IN ('0', '100')
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key
WHERE a.key1 NOT IN ('0', '100')
ORDER BY a.key1
LIMIT 20;

-- Using only key1, with a predicate with key2
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key
WHERE a.key2 NOT IN ('val_0', 'val_100')
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key
WHERE a.key2 NOT IN ('val_0', 'val_100')
ORDER BY a.key1
LIMIT 20;

-- Using only key2
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.key2 = b.value
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.key2 = b.value
ORDER BY a.key1
LIMIT 20;

-- Using both key1, key2, and an additional column
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key AND a.key2 = b.value AND a.value = b.value
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.key1 = b.key AND a.key2 = b.value AND a.value = b.value
ORDER BY a.key1
LIMIT 20;

-- Using only a non bucketing column
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src b ON a.value = b.value
ORDER BY a.key1
LIMIT 20;

SELECT *
FROM srcbucket_big a
JOIN src b ON a.value = b.value
ORDER BY a.key1
LIMIT 20;
