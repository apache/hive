set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.listbucketing=true;
set mapred.input.dir.recursive=true;	
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)	

-- List bucketing query logic test case. We simulate the directory structure by DML here.
-- Test condition: 
-- 1. where clause has multiple skewed columns and non-skewed columns
-- 3. where clause has a few operators
-- Test focus:
-- 1. query works for on partition level. 
--    A table can mix up non-skewed partition and skewed partition
--    Even for skewed partition, it can have different skewed information.
-- Test result:
-- 1. pruner only pick up right directory
-- 2. query result is right

-- create 2 tables: fact_daily and fact_daily
-- fact_daily will be used for list bucketing query
-- fact_daily is a table used to prepare data and test directories		
CREATE TABLE fact_daily(x int, y STRING, z STRING) PARTITIONED BY (ds STRING, hr STRING)	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_daily';	

-- create /fact_daily/ds=1/hr=1 directory	
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='1', hr='1')	
SELECT key, value, value FROM src WHERE key=484;	

-- create /fact_daily/ds=1/hr=2 directory	
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='1', hr='2')	
SELECT key+11, value, value FROM src WHERE key=484;

-- create /fact_daily/ds=1/hr=3 directory	
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='1', hr='3')	
SELECT key, value, value FROM src WHERE key=238;

-- create /fact_daily/ds=1/hr=4 directory	
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='1', hr='4')	
SELECT key, value, value FROM src WHERE key=98;

dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=1 ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5/x=484/y=val_484;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=2 ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5/x=495/y=val_484;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=3 ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5/x=238/y=val_238;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=4 ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME;
dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1;	

-- create a non-skewed partition ds=200 and hr =1 in fact_daily table
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='200', hr='1') SELECT key, value, value FROM src WHERE key=145 or key=406 or key=429;

-- switch fact_daily to skewed table, create partition ds=1 and hr=5 and point its location to /fact_daily/ds=1
alter table fact_daily skewed by (x,y) on ((484,'val_484'),(238,'val_238'));
ALTER TABLE fact_daily SET TBLPROPERTIES('EXTERNAL'='TRUE');	
ALTER TABLE fact_daily ADD PARTITION (ds='1', hr='5')	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5';	

-- set List Bucketing location map
alter table fact_daily PARTITION (ds = '1', hr='5') set skewed location ((484,'val_484')='${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5/x=484/y=val_484',
(238,'val_238')='${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5/x=238/y=val_238');
describe formatted fact_daily PARTITION (ds = '1', hr='5');

-- alter skewed information and create partition ds=100 and hr=1
alter table fact_daily skewed by (x,y) on ((495,'val_484'));
ALTER TABLE fact_daily ADD PARTITION (ds='100', hr='1')	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5';
alter table fact_daily PARTITION (ds = '100', hr='1') set skewed location ((495,'val_484')='${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=5/x=495/y=val_484');
describe formatted fact_daily PARTITION (ds = '100', hr='1');


-- query non-skewed partition
explain extended
select * from fact_daily where ds='200' and  hr='1' and x=145;
select * from fact_daily where ds='200' and  hr='1' and x=145;
explain extended
select * from fact_daily where ds='200' and  hr='1';
select * from fact_daily where ds='200' and  hr='1';
	
-- query skewed partition
explain extended
SELECT * FROM fact_daily WHERE ds='1' and hr='5' and (x=484 and y ='val_484');	
SELECT * FROM fact_daily WHERE ds='1' and hr='5' and (x=484 and y ='val_484');

-- query another skewed partition
explain extended
SELECT * FROM fact_daily WHERE ds='100' and hr='1' and (x=495 and y ='val_484');	
SELECT * FROM fact_daily WHERE ds='100' and hr='1' and (x=495 and y ='val_484');	
