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
create table mysource (p int, key int, value int);
insert into mysource values (100,20,201), (101,40,401), (102,50,501);
insert into mysource values (100,21,211), (101,41,411), (102,51,511);

--explain select count(*) from mysource;
--select count(*) from mysource;

-- Gather col stats manually
--analyze table mysource compute statistics for columns p, key;

--explain select count(*) from mysource;
--select count(*) from mysource;
--explain select count(key) from mysource;
--select count(key) from mysource;

-- test partitioned table
drop table if exists stats_partitioned;

--create table stats_part(key int,value string) partitioned by (p int) stored as orc;
create table stats_part(key int,value string) partitioned by (p int) stored as orc tblproperties ("transactional"="true");
--create table stats_part(key int,value string) partitioned by (p int) stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");

explain select count(key) from stats_part;
--select count(*) from stats_part;
--explain select count(*) from stats_part where p = 100;
--select count(*) from stats_part where p = 100;
explain select count(key) from stats_part where p > 100;
--select count(*) from stats_part where p > 100;
desc formatted stats_part;

--explain insert into table stats_part partition(p=100) select distinct key, value from mysource where p == 100;
insert into table stats_part partition(p=100) select distinct key, value from mysource where p == 100;
insert into table stats_part partition(p=101) select distinct key, value from mysource where p == 101;
insert into table stats_part partition(p=102) select distinct key, value from mysource where p == 102;

desc formatted stats_part;

insert into table mysource values (103,20,200), (103,83,832), (103,53,530);
insert into table stats_part partition(p=102) select distinct key, value from mysource where p == 102;

desc formatted stats_part;
show partitions stats_part;

explain select count(*) from stats_part;
select count(*) from stats_part;
explain select count(key) from stats_part;
select count(key) from stats_part;
explain select count(key) from stats_part where p > 100;
select count(key) from stats_part where p > 100;
explain select max(key) from stats_part where p > 100;
select max(key) from stats_part where p > 100;

--update stats_part set key = key + 100 where key in(-50,40) and p > 100;
desc formatted stats_part;
explain select max(key) from stats_part where p > 100;
select max(key) from stats_part where p > 100;

select count(value) from stats_part;
--update stats_part set value = concat(value, 'updated') where cast(key as integer) in(40,53) and p > 100;
select count(value) from stats_part;

--delete from stats_part where key in (20, 41); 
desc formatted stats_part;

explain select count(*) from stats_part where p = 100;
select count(*) from stats_part where p = 100;
explain select count(*) from stats_part where p > 100;
select count(*) from stats_part where p > 100;
explain select count(key) from stats_part;
select count(key) from stats_part;
explain select count(*) from stats_part where p > 100;
select count(*) from stats_part where p > 100;
explain select max(key) from stats_part where p > 100;
select max(key) from stats_part where p > 100;

describe extended stats_part partition (p=101);
describe extended stats_part;


