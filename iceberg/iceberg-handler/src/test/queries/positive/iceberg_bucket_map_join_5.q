-- Copied from bucketmapjoin12.q
set hive.explain.user=true;

--! qt:dataset:part
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;

CREATE TABLE srcbucket_mapjoin_part_1_tmp (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_tmp PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_tmp PARTITION (part='1');
CREATE TABLE srcbucket_mapjoin_part_1 (key INT, value STRING, part STRING) PARTITIONED BY SPEC (part, bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_1 SELECT * FROM srcbucket_mapjoin_part_1_tmp;

CREATE TABLE srcbucket_mapjoin_part_2_n0_tmp (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n0_tmp PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n0_tmp PARTITION (part='1');
CREATE TABLE srcbucket_mapjoin_part_2_n0 (key INT, value STRING, part STRING) PARTITIONED BY SPEC (part, bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_2_n0 SELECT * FROM srcbucket_mapjoin_part_2_n0_tmp;

ALTER TABLE srcbucket_mapjoin_part_2_n0 SET PARTITION SPEC (part);

CREATE TABLE srcbucket_mapjoin_part_3_tmp (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_3_tmp PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_3_tmp PARTITION (part='1');
CREATE TABLE srcbucket_mapjoin_part_3 (key INT, value STRING, part STRING) PARTITIONED BY SPEC (part) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_3 SELECT * FROM srcbucket_mapjoin_part_3_tmp;

ALTER TABLE srcbucket_mapjoin_part_3 SET PARTITION SPEC (part, bucket(2, key));
set hive.convert.join.bucket.mapjoin.tez=true;

-- The partition bucketing metadata match but one table is not bucketed, bucket map join should still be used

EXPLAIN
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2_n0 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2_n0 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

-- The table bucketing metadata match and one partition is not bucketed, bucket map join should be used

EXPLAIN
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_3 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_3 b
ON a.key = b.key AND a.part = '1' and b.part = '1';
