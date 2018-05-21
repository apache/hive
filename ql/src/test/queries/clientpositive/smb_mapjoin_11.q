--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

set hive.cbo.enable=false;

set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- This test verifies that the output of a sort merge join on 2 partitions (one on each side of the join) is bucketed

-- Create two bucketed and sorted tables
CREATE TABLE test_table1_n1 (key INT, value STRING) PARTITIONED BY (ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 16 BUCKETS;
CREATE TABLE test_table2_n1 (key INT, value STRING) PARTITIONED BY (ds STRING) CLUSTERED BY (key) SORTED BY (key) INTO 16 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n1 PARTITION (ds = '1') SELECT *
INSERT OVERWRITE TABLE test_table2_n1 PARTITION (ds = '1') SELECT *;




-- Create a bucketed table
CREATE TABLE test_table3_n1 (key INT, value STRING) PARTITIONED BY (ds STRING) CLUSTERED BY (key) INTO 16 BUCKETS;

-- Insert data into the bucketed table by joining the two bucketed and sorted tables, bucketing is not enforced
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE test_table3_n1 PARTITION (ds = '1') SELECT /*+ MAPJOIN(b) */ a.key, b.value FROM test_table1_n1 a JOIN test_table2_n1 b ON a.key = b.key AND a.ds = '1' AND b.ds = '1';

INSERT OVERWRITE TABLE test_table3_n1 PARTITION (ds = '1') SELECT /*+ MAPJOIN(b) */ a.key, b.value FROM test_table1_n1 a JOIN test_table2_n1 b ON a.key = b.key AND a.ds = '1' AND b.ds = '1';

SELECT * FROM test_table1_n1 ORDER BY key;
SELECT * FROM test_table3_n1 ORDER BY key;
EXPLAIN EXTENDED SELECT * FROM test_table1_n1 TABLESAMPLE(BUCKET 2 OUT OF 16);
EXPLAIN EXTENDED SELECT * FROM test_table3_n1 TABLESAMPLE(BUCKET 2 OUT OF 16);
SELECT * FROM test_table1_n1 TABLESAMPLE(BUCKET 2 OUT OF 16);
SELECT * FROM test_table3_n1 TABLESAMPLE(BUCKET 2 OUT OF 16);

-- Join data from a sampled bucket to verify the data is bucketed
SELECT COUNT(*) FROM test_table3_n1 TABLESAMPLE(BUCKET 2 OUT OF 16) a JOIN test_table1_n1 TABLESAMPLE(BUCKET 2 OUT OF 16) b ON a.key = b.key AND a.ds = '1' AND b.ds='1';

