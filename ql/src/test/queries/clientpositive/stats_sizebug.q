set hive.stats.dbclass=fs;
set hive.stats.fetch.column.stats=true;
set datanucleus.cache.collections=false;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
set hive.compute.query.using.stats=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.query.results.cache.enabled=false;

-- create source.
drop table if exists mysource;
create table mysource (p int,key int);
insert into mysource values (100,20), (101,40), (102,50);
insert into mysource values (100,20), (101,40), (102,50);

-- test nonpartitioned table
drop table if exists stats_nonpartitioned;

--create table stats_nonpartitioned(key int, value int) stored as orc;
create table stats_nonpartitioned(key int, value int) stored as orc tblproperties ("transactional"="true");
--create table stats_nonpartitioned(key int, value int) stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");
explain insert into table stats_nonpartitioned select * from mysource where p == 100; 
insert into table stats_nonpartitioned select * from mysource where p == 100; 

desc formatted stats_nonpartitioned;
analyze table mysource compute statistics for columns p, key;
desc formatted stats_nonpartitioned;


