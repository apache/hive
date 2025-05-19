-- Copied from bucketmapjoin11.q
set hive.explain.user=true;

--! qt:dataset:part
set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=false;

CREATE TABLE srcbucket_mapjoin_part_1_n2_tmp1 (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_n2_tmp1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_n2_tmp1 PARTITION (part='1');
CREATE TABLE srcbucket_mapjoin_part_1_n2 (key INT, value STRING, part STRING) PARTITIONED BY SPEC (part, bucket(2, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_1_n2 SELECT * FROM srcbucket_mapjoin_part_1_n2_tmp1;

ALTER TABLE srcbucket_mapjoin_part_1_n2 SET PARTITION SPEC (part, bucket(4, key));
CREATE TABLE srcbucket_mapjoin_part_1_n2_tmp2 (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_n2_tmp2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_n2_tmp2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_1_n2_tmp2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_1_n2_tmp2 PARTITION (part='2');
INSERT INTO srcbucket_mapjoin_part_1_n2 SELECT * FROM srcbucket_mapjoin_part_1_n2_tmp2;

CREATE TABLE srcbucket_mapjoin_part_2_n6_tmp1 (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n6_tmp1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n6_tmp1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_2_n6_tmp1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_2_n6_tmp1 PARTITION (part='1');
CREATE TABLE srcbucket_mapjoin_part_2_n6 (key INT, value STRING, part STRING) PARTITIONED BY SPEC (part, bucket(4, key)) STORED BY ICEBERG;
INSERT INTO srcbucket_mapjoin_part_2_n6 SELECT * FROM srcbucket_mapjoin_part_2_n6_tmp1;

ALTER TABLE srcbucket_mapjoin_part_2_n6 SET PARTITION SPEC (part, bucket(2, key));
CREATE TABLE srcbucket_mapjoin_part_2_n6_tmp2 (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n6_tmp2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n6_tmp2 PARTITION (part='2');
INSERT INTO srcbucket_mapjoin_part_2_n6 SELECT * FROM srcbucket_mapjoin_part_2_n6_tmp2;

set hive.convert.join.bucket.mapjoin.tez=true;

-- The partition spec has been changed. This pattern is not supported yet

EXPLAIN
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part IS NOT NULL AND b.part IS NOT NULL;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part IS NOT NULL AND b.part IS NOT NULL;

EXPLAIN
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part = b.part AND a.part IS NOT NULL AND b.part IS NOT NULL;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part = b.part AND a.part IS NOT NULL AND b.part IS NOT NULL;
