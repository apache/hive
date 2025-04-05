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
ALTER TABLE srcbucket_big CREATE TAG bucket_4;

ALTER TABLE srcbucket_big SET PARTITION SPEC (bucket(8, key));
INSERT INTO srcbucket_big VALUES
(101, 'val_101', 1),
(null, 'val_102', 2),
(103, 'val_103', 3),
(104, null, 4),
(105, 'val_105', 5),
(null, null, 6);
ALTER TABLE srcbucket_big CREATE TAG bucket_4_and_8;

CREATE TABLE src_small(key int, value string);
INSERT INTO src_small VALUES
(101, 'val_101'),
(null, 'val_102'),
(103, 'val_103'),
(104, null),
(105, 'val_105'),
(null, null);

-- Both bucket(4, key) and bucket(8, key) belong to the current snapshot
EXPLAIN
SELECT *
FROM default.srcbucket_big a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

SELECT *
FROM default.srcbucket_big a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

-- Only bucket(4, key) belongs to buckets_4
EXPLAIN
SELECT *
FROM default.srcbucket_big.tag_bucket_4 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

SELECT *
FROM default.srcbucket_big.tag_bucket_4 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

-- Both bucket(4, key) and bucket(8, key) belong to buckets_4_and_8
EXPLAIN
SELECT *
FROM default.srcbucket_big.tag_bucket_4_and_8 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

SELECT *
FROM default.srcbucket_big.tag_bucket_4_and_8 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

alter table srcbucket_big compact 'major' and wait;

-- Only bucket(8, key) belongs to the current snapshot thanks to the compaction
EXPLAIN
SELECT *
FROM default.srcbucket_big a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

SELECT *
FROM default.srcbucket_big a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

-- Only bucket(4, key) belongs to buckets_4
EXPLAIN
SELECT *
FROM default.srcbucket_big.tag_bucket_4 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

SELECT *
FROM default.srcbucket_big.tag_bucket_4 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

-- Both bucket(4, key) and bucket(8, key) belong to buckets_4_and_8
EXPLAIN
SELECT *
FROM default.srcbucket_big.tag_bucket_4_and_8 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;

SELECT *
FROM default.srcbucket_big.tag_bucket_4_and_8 a
JOIN default.src_small b ON a.key = b.key
ORDER BY a.id;
