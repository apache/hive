--! qt:dataset:part
SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE srcbucket_mapjoin_part_1 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1 PARTITION (part='1');

CREATE TABLE srcbucket_mapjoin_part_2_n0 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n0 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n0 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_2_n0 NOT CLUSTERED;

CREATE TABLE srcbucket_mapjoin_part_3 (key INT, value STRING) PARTITIONED BY (part STRING)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_3 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_3 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_3 CLUSTERED BY (key) INTO 2 BUCKETS;
set hive.cbo.enable=false;
set hive.optimize.bucketmapjoin=true;

-- The partition bucketing metadata match but one table is not bucketed, bucket map join should still be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2_n0 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_2_n0 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

-- The table bucketing metadata match but one partition is not bucketed, bucket map join should not be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_3 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1 a JOIN srcbucket_mapjoin_part_3 b
ON a.key = b.key AND a.part = '1' and b.part = '1';
