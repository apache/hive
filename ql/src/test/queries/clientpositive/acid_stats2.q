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

set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.query.results.cache.enabled=false;

create table stats3(key int,value string) stored as orc tblproperties ("transactional"="true");
insert into table stats3  values (1, "foo");
explain select count(*) from stats3;
select count(*) from stats3;
insert into table stats3  values (2, "bar");
explain select count(*) from stats3;
select count(*) from stats3;
update stats3 set value = "baz" where key = 4;
explain select count(*) from stats3;
select count(*) from stats3;
update stats3 set value = "baz" where key = 1;
explain select count(*) from stats3;
select count(*) from stats3;
delete from stats3 where key = 3;
explain select count(*) from stats3;
select count(*) from stats3;
delete from stats3 where key = 1;
explain select count(*) from stats3;
select count(*) from stats3;
delete from stats3 where key = 2;
explain select count(*) from stats3;
select count(*) from stats3;

drop table stats3;