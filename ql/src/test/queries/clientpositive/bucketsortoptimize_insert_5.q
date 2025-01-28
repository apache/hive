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
CREATE TABLE test_table1_n8 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2_n8 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table3_n5 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key desc) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n8 PARTITION (ds = '1') SELECT * where key < 10;

FROM src
INSERT OVERWRITE TABLE test_table2_n8 PARTITION (ds = '1') SELECT * where key < 100;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-reduce operation, since the sort-order does not match
EXPLAIN
INSERT OVERWRITE TABLE test_table3_n5 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM test_table1_n8 a JOIN test_table2_n8 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1';

INSERT OVERWRITE TABLE test_table3_n5 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM test_table1_n8 a JOIN test_table2_n8 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1';

select * from test_table3_n5 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3_n5 tablesample (bucket 2 out of 2) s where ds = '1';

-- This should be a map-reduce job since the sort order does not match
EXPLAIN
INSERT OVERWRITE TABLE test_table3_n5 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM 
(select key, value from test_table1_n8 where ds = '1') a 
JOIN 
(select key, value from test_table2_n8 where ds = '1') b 
ON a.key = b.key;

INSERT OVERWRITE TABLE test_table3_n5 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM 
(select key, value from test_table1_n8 where ds = '1') a 
JOIN 
(select key, value from test_table2_n8 where ds = '1') b 
ON a.key = b.key;

select * from test_table3_n5 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3_n5 tablesample (bucket 2 out of 2) s where ds = '1';
