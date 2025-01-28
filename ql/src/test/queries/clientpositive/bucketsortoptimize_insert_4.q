--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;


set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 
set hive.auto.convert.sortmerge.join.bigtable.selection.policy=org.apache.hadoop.hive.ql.optimizer.LeftmostBigTableSelectorForAutoSMJ;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;

-- Create two bucketed and sorted tables
CREATE TABLE test_table1_n16 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2_n15 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table3_n8 (key INT, key2 INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key2) SORTED BY (key2) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n16 PARTITION (ds = '1') SELECT * where key < 10;

FROM src
INSERT OVERWRITE TABLE test_table2_n15 PARTITION (ds = '1') SELECT * where key < 100;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation, since the insert is happening on the bucketing position
EXPLAIN
INSERT OVERWRITE TABLE test_table3_n8 PARTITION (ds = '1')
SELECT a.key, a.key, concat(a.value, b.value) 
FROM test_table1_n16 a JOIN test_table2_n15 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1';

INSERT OVERWRITE TABLE test_table3_n8 PARTITION (ds = '1')
SELECT a.key, a.key, concat(a.value, b.value) 
FROM test_table1_n16 a JOIN test_table2_n15 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1';

select * from test_table3_n8 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3_n8 tablesample (bucket 2 out of 2) s where ds = '1';

DROP TABLE test_table3_n8;

CREATE TABLE test_table3_n8 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation, since the insert is happening on a non-bucketing position
EXPLAIN
INSERT OVERWRITE TABLE test_table3_n8 PARTITION (ds = '1')
SELECT a.key, a.value
FROM test_table1_n16 a JOIN test_table2_n15 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1';

INSERT OVERWRITE TABLE test_table3_n8 PARTITION (ds = '1')
SELECT a.key, a.value
FROM test_table1_n16 a JOIN test_table2_n15 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1';

select * from test_table3_n8 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3_n8 tablesample (bucket 2 out of 2) s where ds = '1';

DROP TABLE test_table3_n8;
