--! qt:dataset:src
set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.convert.join.bucket.mapjoin.tez=true;

CREATE TABLE srcbucket_big(key1 int, key2 string, value string, id int)
PARTITIONED BY SPEC(bucket(4, key1), bucket(8, key2)) STORED BY ICEBERG;
INSERT INTO srcbucket_big VALUES
(101, '1001', 'val_101', 1),
(null, '1002', 'val_102', 2),
(103, null, 'val_103', 3),
(104, '1004', null, 4),
(105, '1005', 'val_105', 5),
(101, '1001', 'val_101', 6),
(null, '1002', 'val_102', 7),
(103, null, 'val_103', 8),
(104, '1004', null, 9),
(105, '1005', 'val_105', 10),
(101, '1001', 'val_101', 11),
(null, '1002', 'val_102', 12),
(103, null, 'val_103', 13),
(104, '1004', null, 14),
(105, '1005', 'val_105', 15),
(101, '1001', 'val_101', 16),
(null, '1002', 'val_102', 17),
(103, null, 'val_103', 18),
(104, '1004', null, 19),
(105, '1005', 'val_105', 20),
(null, null, null, 21);

CREATE TABLE src_small(key1 int, key2 string, value string);
INSERT INTO src_small VALUES
(101, '1001', 'val_101'),
(null, '1002', 'val_102'),
(103, null, 'val_103'),
(104, '1004', null),
(105, '1005', 'val_105'),
(null, null, null);

SELECT * FROM srcbucket_big ORDER BY id;

-- key1
EXPLAIN
SELECT a.key1, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1
ORDER BY a.id;

SELECT a.key1, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1
ORDER BY a.id;

-- key2
EXPLAIN
SELECT a.key2, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key2 = b.key2
ORDER BY a.id;

SELECT a.key2, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key2 = b.key2
ORDER BY a.id;

-- Using a non-partition column
EXPLAIN
SELECT a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.value = b.value
ORDER BY a.id;

SELECT a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.value = b.value
ORDER BY a.id;

-- key1 & key2
EXPLAIN
SELECT a.key1, a.key2, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1 AND a.key2 = b.key2
ORDER BY a.id;

SELECT a.key1, a.key2, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1 AND a.key2 = b.key2
ORDER BY a.id;

-- key1 & non-partition column
EXPLAIN
SELECT a.key1, a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1 AND a.value = b.value
ORDER BY a.id;

SELECT a.key1, a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1 AND a.value = b.value
ORDER BY a.id;

-- key2 & non-partition column
EXPLAIN
SELECT a.key2, a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key2 = b.key2 AND a.value = b.value
ORDER BY a.id;

SELECT a.key2, a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key2 = b.key2 AND a.value = b.value
ORDER BY a.id;

-- key1 & key2 & non-partition column
EXPLAIN
SELECT a.key1, a.key2, a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1 AND a.key2 = b.key2 AND a.value = b.value
ORDER BY a.id;

SELECT a.key1, a.key2, a.value, a.id
FROM srcbucket_big a
JOIN src_small b ON a.key1 = b.key1 AND a.key2 = b.key2 AND a.value = b.value
ORDER BY a.id;
