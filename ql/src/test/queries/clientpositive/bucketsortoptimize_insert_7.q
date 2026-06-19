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
-- disable hash joins
set hive.auto.convert.join.noconditionaltask.size=10;

-- Create two bucketed and sorted tables
CREATE TABLE test_table1_n20 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2_n19 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table3_n11 (key INT, value STRING) PARTITIONED BY (ds STRING)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

FROM src
INSERT OVERWRITE TABLE test_table1_n20 PARTITION (ds = '1') SELECT * where key < 10;

FROM src
INSERT OVERWRITE TABLE test_table2_n19 PARTITION (ds = '1') SELECT * where key < 100;

-- Insert data into the bucketed table by selecting from another bucketed table
-- This should be a map-only operation
EXPLAIN
INSERT OVERWRITE TABLE test_table3_n11 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM test_table1_n20 a JOIN test_table2_n19 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1'
and (a.key = 0 or a.key = 5);

INSERT OVERWRITE TABLE test_table3_n11 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM test_table1_n20 a JOIN test_table2_n19 b 
ON a.key = b.key WHERE a.ds = '1' and b.ds = '1'
and (a.key = 0 or a.key = 5);

select * from test_table3_n11 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3_n11 tablesample (bucket 2 out of 2) s where ds = '1';

-- This should be a map-only job
EXPLAIN
INSERT OVERWRITE TABLE test_table3_n11 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM 
(select key, value from test_table1_n20 where ds = '1' and (key = 0 or key = 5)) a 
JOIN 
(select key, value from test_table2_n19 where ds = '1' and (key = 0 or key = 5)) b 
ON a.key = b.key;

INSERT OVERWRITE TABLE test_table3_n11 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM 
(select key, value from test_table1_n20 where ds = '1' and (key = 0 or key = 5)) a 
JOIN 
(select key, value from test_table2_n19 where ds = '1' and (key = 0 or key = 5)) b 
ON a.key = b.key;

select * from test_table3_n11 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3_n11 tablesample (bucket 2 out of 2) s where ds = '1';

-- This should be a map-only job
EXPLAIN
INSERT OVERWRITE TABLE test_table3_n11 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM 
(select key, value from test_table1_n20 where ds = '1' and key < 8) a 
JOIN 
(select key, value from test_table2_n19 where ds = '1' and key < 8) b 
ON a.key = b.key
WHERE a.key = 0 or a.key = 5;

INSERT OVERWRITE TABLE test_table3_n11 PARTITION (ds = '1')
SELECT a.key, concat(a.value, b.value) 
FROM 
(select key, value from test_table1_n20 where ds = '1' and key < 8) a 
JOIN 
(select key, value from test_table2_n19 where ds = '1' and key < 8) b 
ON a.key = b.key
WHERE a.key = 0 or a.key = 5;

select * from test_table3_n11 tablesample (bucket 1 out of 2) s where ds = '1';
select * from test_table3_n11 tablesample (bucket 2 out of 2) s where ds = '1';
