-- Copied from bucketmapjoin8.q
set hive.explain.user=true;

--! qt:dataset:part
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;

CREATE TABLE srcbucket_mapjoin_part_1_n1_tmp (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_n1_tmp PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_n1_tmp PARTITION (part='1');
CREATE TABLE srcbucket_mapjoin_part_1_n1 (key INT, value STRING, part STRING) PARTITIONED BY SPEC (part, bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_1_n1 SELECT * FROM srcbucket_mapjoin_part_1_n1_tmp;

CREATE TABLE srcbucket_mapjoin_part_2_n4_tmp (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n4_tmp PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n4_tmp PARTITION (part='1');
CREATE TABLE srcbucket_mapjoin_part_2_n4 (key INT, value STRING, part STRING) PARTITIONED BY SPEC (part, bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_2_n4 SELECT * FROM srcbucket_mapjoin_part_2_n4_tmp;

ALTER TABLE srcbucket_mapjoin_part_2_n4 SET PARTITION SPEC (part, bucket(3, key));

set hive.convert.join.bucket.mapjoin.tez=true;
-- The partition bucketing metadata match but the tables have different numbers of buckets, bucket map join should still be used

EXPLAIN
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

ALTER TABLE srcbucket_mapjoin_part_2_n4 SET PARTITION SPEC (part, bucket(2, value));

-- The partition bucketing metadata match but the tables are bucketed on different columns, bucket map join should still be used

EXPLAIN
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';
