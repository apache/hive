--! qt:dataset:part
SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE srcbucket_mapjoin_part_1_n1 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_n1 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_n1 PARTITION (part='1');

CREATE TABLE srcbucket_mapjoin_part_2_n4 (key INT, value STRING) PARTITIONED BY (part STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n4 PARTITION (part='1');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n4 PARTITION (part='1');

ALTER TABLE srcbucket_mapjoin_part_2_n4 CLUSTERED BY (key) INTO 3 BUCKETS;

set hive.optimize.bucketmapjoin=true;
set hive.cbo.enable=false;
-- The partition bucketing metadata match but the tables have different numbers of buckets, bucket map join should still be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

ALTER TABLE srcbucket_mapjoin_part_2_n4 CLUSTERED BY (value) INTO 2 BUCKETS;

-- The partition bucketing metadata match but the tables are bucketed on different columns, bucket map join should still be used

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';

SELECT /*+ MAPJOIN(b) */ count(*)
FROM srcbucket_mapjoin_part_1_n1 a JOIN srcbucket_mapjoin_part_2_n4 b
ON a.key = b.key AND a.part = '1' and b.part = '1';
