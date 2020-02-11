--! qt:dataset:srcpart
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
set hive.exec.infer.bucket.sort=true;
set hive.exec.infer.bucket.sort.num.buckets.power.two=true;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

-- This tests inferring how data is bucketed/sorted from the operators in the reducer
-- and populating that information in partitions' metadata.  In particular, those cases
-- where dynamic partitioning is used.

CREATE TABLE test_table_n8 LIKE srcpart;
ALTER TABLE test_table_n8 SET FILEFORMAT RCFILE;

-- Simple case, this should not be bucketed or sorted

INSERT OVERWRITE TABLE test_table_n8 PARTITION (ds, hr)
SELECT key, value, ds, hr FROM srcpart
WHERE ds = '2008-04-08';

DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='11');
DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='12');

-- This should not be bucketed or sorted since the partition keys are in the set of bucketed
-- and sorted columns for the output 

INSERT OVERWRITE TABLE test_table_n8 PARTITION (ds, hr)
SELECT key, COUNT(*), ds, hr FROM srcpart
WHERE ds = '2008-04-08'
GROUP BY key, ds, hr;

DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='11');
DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='12');

-- Both partitions should be bucketed and sorted by key

INSERT OVERWRITE TABLE test_table_n8 PARTITION (ds, hr)
SELECT key, value, '2008-04-08', IF (key % 2 == 0, '11', '12') FROM
(SELECT key, COUNT(*) AS value FROM srcpart
WHERE ds = '2008-04-08'
GROUP BY key) a;

DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='11');
DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='12');

CREATE TABLE srcpart_merge_dp_n3 LIKE srcpart;

CREATE TABLE srcpart_merge_dp_rc_n0 LIKE srcpart;
ALTER TABLE srcpart_merge_dp_rc_n0 SET FILEFORMAT RCFILE;

LOAD DATA LOCAL INPATH '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp_n3 PARTITION(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '../../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp_n3 PARTITION(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '../../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp_n3 PARTITION(ds='2008-04-08', hr=11);
LOAD DATA LOCAL INPATH '../../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp_n3 PARTITION(ds='2008-04-08', hr=11);

LOAD DATA LOCAL INPATH '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp_n3 PARTITION(ds='2008-04-08', hr=12);

INSERT OVERWRITE TABLE srcpart_merge_dp_rc_n0 PARTITION (ds = '2008-04-08', hr) 
SELECT key, value, hr FROM srcpart_merge_dp_n3 WHERE ds = '2008-04-08';

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=200;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set mapred.reduce.tasks=2;

-- Tests dynamic partitions where bucketing/sorting can be inferred, but some partitions are
-- merged and some are moved.  Currently neither should be bucketed or sorted, in the future,
-- (ds='2008-04-08', hr='12') may be bucketed and sorted, (ds='2008-04-08', hr='11') should
-- definitely not be.

EXPLAIN
INSERT OVERWRITE TABLE test_table_n8 PARTITION (ds = '2008-04-08', hr)
SELECT key, value, IF (key % 100 == 0, '11', '12') FROM
(SELECT key, COUNT(*) AS value FROM srcpart
WHERE ds = '2008-04-08'
GROUP BY key) a;

INSERT OVERWRITE TABLE test_table_n8 PARTITION (ds = '2008-04-08', hr)
SELECT key, value, IF (key % 100 == 0, '11', '12') FROM
(SELECT key, COUNT(*) AS value FROM srcpart
WHERE ds = '2008-04-08'
GROUP BY key) a;

DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='11');
DESCRIBE FORMATTED test_table_n8 PARTITION (ds='2008-04-08', hr='12');
