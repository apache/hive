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

-- test simple partition case

create table stats_part(key int,value string) partitioned by (p int) tblproperties ("transactional"="true", "transactional_properties"="insert_only");

insert into table stats_part partition(p=101) values (1, "foo");
explain select count(key) from stats_part;
insert into table stats_part partition(p=102) values (1, "bar");
explain select count(key) from stats_part;

alter table stats_part drop partition (p=102);
explain select count(key) from stats_part;

drop table stats_part;

