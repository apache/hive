--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.listbucketing=true;
set mapred.input.dir.recursive=true;	
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)	
-- SORT_QUERY_RESULTS

-- List bucketing query logic test case. 
-- Test condition: 
-- 1. where clause has only one skewed column
-- 2. where clause doesn't have non-skewed column
-- 3. where clause has one and operator
-- Test result:
-- 1. pruner only pick up right directory
-- 2. query result is right

-- create 2 tables: fact_daily_n4 and fact_tz_n1
-- fact_daily_n4 will be used for list bucketing query
-- fact_tz_n1 is a table used to prepare data and test directories	
CREATE TABLE fact_daily_n4(x int) PARTITIONED BY (ds STRING);	
CREATE TABLE fact_tz_n1(x int) PARTITIONED BY (ds STRING, hr STRING)	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_tz';	

-- create /fact_tz/ds=1/hr=1 directory	
INSERT OVERWRITE TABLE fact_tz_n1 PARTITION (ds='1', hr='1')	
SELECT key FROM src WHERE key=484;	

-- create /fact_tz/ds=1/hr=2 directory	
INSERT OVERWRITE TABLE fact_tz_n1 PARTITION (ds='1', hr='2')	
SELECT key+11 FROM src WHERE key=484;

dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/hr=1 ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=484;
dfs -mv ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/hr=2 ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME;
dfs -lsr ${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1;

-- switch fact_daily_n4 to skewed table and point its location to /fact_tz/ds=1
alter table fact_daily_n4 skewed by (x) on (484);
ALTER TABLE fact_daily_n4 SET TBLPROPERTIES('EXTERNAL'='TRUE');	
ALTER TABLE fact_daily_n4 ADD PARTITION (ds='1')	
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1';	

-- set List Bucketing location map
alter table fact_daily_n4 PARTITION (ds = '1') set skewed location (484='${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/x=484','HIVE_DEFAULT_LIST_BUCKETING_KEY'='${hiveconf:hive.metastore.warehouse.dir}/fact_tz/ds=1/HIVE_DEFAULT_LIST_BUCKETING_DIR_NAME');
describe formatted fact_daily_n4 PARTITION (ds = '1');
	
SELECT * FROM fact_daily_n4 WHERE ds='1';

-- pruner only pick up skewed-value directory
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT x FROM fact_daily_n4 WHERE ds='1' and x=484;
-- List Bucketing Query
SELECT x FROM fact_daily_n4 WHERE ds='1' and x=484;

-- pruner only pick up default directory since x equal to non-skewed value
-- explain plan shows which directory selected: Truncated Path -> Alias
explain extended SELECT x FROM fact_daily_n4 WHERE ds='1' and x=495;
-- List Bucketing Query
SELECT x FROM fact_daily_n4 WHERE ds='1' and x=495;
explain extended SELECT x FROM fact_daily_n4 WHERE ds='1' and x=1;
SELECT x FROM fact_daily_n4 WHERE ds='1' and x=1;
