SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;
set hive.cbo.enable=false;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE srcbucket_mapjoin_part_1_n4 (key INT, value STRING) PARTITIONED BY (ds STRING, hr STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_1_n4 PARTITION (ds='2008-04-08', hr='0');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_1_n4 PARTITION (ds='2008-04-08', hr='0');

CREATE TABLE srcbucket_mapjoin_part_2_n9 (key INT, value STRING) PARTITIONED BY (ds STRING, hr STRING) 
CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/bmj/000000_0' INTO TABLE srcbucket_mapjoin_part_2_n9 PARTITION (ds='2008-04-08', hr='0');
LOAD DATA LOCAL INPATH '../../data/files/bmj/000001_0' INTO TABLE srcbucket_mapjoin_part_2_n9 PARTITION (ds='2008-04-08', hr='0');

set hive.optimize.bucketmapjoin=true;

-- Tests that bucket map join works with a table with more than one level of partitioning

EXPLAIN EXTENDED
SELECT /*+ MAPJOIN(b) */ a.key, b.value
FROM srcbucket_mapjoin_part_1_n4 a JOIN srcbucket_mapjoin_part_2_n9 b
ON a.key = b.key AND a.ds = '2008-04-08' AND b.ds = '2008-04-08'
ORDER BY a.key, b.value LIMIT 1;

SELECT /*+ MAPJOIN(b) */ a.key, b.value
FROM srcbucket_mapjoin_part_1_n4 a JOIN srcbucket_mapjoin_part_2_n9 b
ON a.key = b.key AND a.ds = '2008-04-08' AND b.ds = '2008-04-08'
ORDER BY a.key, b.value LIMIT 1;
