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
-- 1. basic list bucketing query work
-- Test result:
-- 1. pruner only pick up right directory
-- 2. query result is right


-- create 1 table: fact_daily
-- 1. create a few partitions
-- 2. dfs move partition according to list bucketing structure (simulate DML) 
--    $/fact_daily/ds=1/hr=4/x=../y=..
--    notes: waste all partitions except ds=1 and hr=4 for list bucketing query test
-- 3. alter it to skewed table and set up location map
-- 4. list bucketing query
-- fact_daily (ds=1 and hr=4) will be used for list bucketing query	
CREATE TABLE fact_daily(x int, y STRING) PARTITIONED BY (ds STRING, hr STRING)	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_daily';	

-- create /fact_daily/ds=1/hr=1 directory	
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='1', hr='1')	
SELECT key, value FROM src WHERE key=484;	

-- create /fact_daily/ds=1/hr=2 directory	
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='1', hr='2')	
SELECT key, value FROM src WHERE key=369 or key=406;

-- create /fact_daily/ds=1/hr=3 directory	
INSERT OVERWRITE TABLE fact_daily PARTITION (ds='1', hr='3')	
SELECT key, value FROM src WHERE key=238;

dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=1 ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=4/x=484/y=val_484;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=2 ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=4/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=3 ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=4/x=238/y=val_238;
dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1;

-- switch fact_daily to skewed table and point its location to /fact_daily/ds=1
alter table fact_daily skewed by (x,y) on ((484,'val_484'),(238,'val_238'));	
ALTER TABLE fact_daily ADD PARTITION (ds='1', hr='4');	

-- set List Bucketing location map
alter table fact_daily PARTITION (ds = '1', hr='4') set skewed location ((484,'val_484')='${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=4/x=484/y=val_484',
(238,'val_238')='${hiveconf:hive.metastore.warehouse.dir}/fact_daily/ds=1/hr=4/x=238/y=val_238');
describe formatted fact_daily PARTITION (ds = '1', hr='4');
	
SELECT * FROM fact_daily WHERE ds='1' and hr='4';	

-- pruner only pick up default directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT x,y FROM fact_daily WHERE ds='1' and hr='4' and y= 'val_484';
-- List Bucketing Query
SELECT x,y FROM fact_daily WHERE ds='1' and hr='4' and y= 'val_484';

-- pruner only pick up default directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT x FROM fact_daily WHERE ds='1' and hr='4' and x= 406;
-- List Bucketing Query
SELECT x,y FROM fact_daily WHERE ds='1' and hr='4' and x= 406;

-- pruner only pick up skewed-value directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT x,y FROM fact_daily WHERE ds='1' and hr='4' and ( (x=484 and y ='val_484')  or (x=238 and y= 'val_238')) ;
-- List Bucketing Query
SELECT x,y FROM fact_daily WHERE ds='1' and hr='4' and ( (x=484 and y ='val_484')  or (x=238 and y= 'val_238')) ;

-- clean up
drop table fact_daily;