--! qt:dataset:src
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.cbo.enable=false;

set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- Create bucketed and sorted tables
CREATE TABLE test_table1_n9 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2_n9 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n9 SELECT *
INSERT OVERWRITE TABLE test_table2_n9 SELECT *;

-- Mapjoin followed by a aggregation should be performed in a single MR job
EXPLAIN
SELECT /*+mapjoin(b)*/ count(*) FROM test_table1_n9 a JOIN test_table2_n9 b ON a.key = b.key;
SELECT /*+mapjoin(b)*/ count(*) FROM test_table1_n9 a JOIN test_table2_n9 b ON a.key = b.key;
