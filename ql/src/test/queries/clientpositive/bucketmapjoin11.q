--! qt:dataset:part
SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE srcbucket_mapjoin_part_1_n2 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_n2 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_n2 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_1_n2 CLUSTERED BY (key) INTO 4 BUCKETS;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_n2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_n2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_1_n2 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_1_n2 PARTITION (part='2');

CREATE TABLE srcbucket_mapjoin_part_2_n6 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 4 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n6 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n6 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000002_0' INTO TABLE srcbucket_mapjoin_part_2_n6 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000003_0' INTO TABLE srcbucket_mapjoin_part_2_n6 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_2_n6 CLUSTERED BY (key) INTO 2 BUCKETS;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n6 PARTITION (part='2');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n6 PARTITION (part='2');

set hive.cbo.enable=false;
set hive.optimize.bucketmapjoin=true;

-- The table and partition bucketing metadata doesn't match but the bucket numbers of all partitions is
-- a power of 2 and the bucketing columns match so bucket map join should be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part IS NOT NULL AND b.part IS NOT NULL;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part IS NOT NULL AND b.part IS NOT NULL;

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part = b.part AND a.part IS NOT NULL AND b.part IS NOT NULL;

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n2 a JOIN srcbucket_mapjoin_part_2_n6 b
ON a.key = b.key AND a.part = b.part AND a.part IS NOT NULL AND b.part IS NOT NULL;
