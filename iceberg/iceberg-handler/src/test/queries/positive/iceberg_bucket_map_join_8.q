--! qt:dataset:src
set hive.explain.user=true;
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.convert.join.bucket.mapjoin.tez=true;

CREATE TABLE srcbucket_big(key int, value string, id int)
PARTITIONED BY SPEC(bucket(4, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_big VALUES
(101, 'val_101', 1),
(null, 'val_102', 2),
(103, 'val_103', 3),
(104, null, 4),
(105, 'val_105', 5),
(null, null, 6);

CREATE TABLE src_small(key int, value string);
INSERT INTO src_small VALUES
(101, 'val_101'),
(null, 'val_102'),
(103, 'val_103'),
(104, null),
(105, 'val_105'),
(null, null);

SELECT * FROM srcbucket_big ORDER BY id;

-- Using the bucket column
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src_small b ON a.key = b.key
ORDER BY a.id;

SELECT *
FROM srcbucket_big a
JOIN src_small b ON a.key = b.key
ORDER BY a.id;

-- Using a non-bucket column
EXPLAIN
SELECT *
FROM srcbucket_big a
JOIN src_small b ON a.value = b.value
ORDER BY a.id;

SELECT *
FROM srcbucket_big a
JOIN src_small b ON a.value = b.value
ORDER BY a.id;
