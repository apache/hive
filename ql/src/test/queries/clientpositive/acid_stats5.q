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

-- Test various scenarios where stats become invalid; verify they are invalid, and that analyze works.

create table stats2(key int,value string) tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table stats2  values (1, "foo");
insert into table stats2  values (2, "bar");
explain select count(*) from stats2;
explain select min(key) from stats2;

set hive.stats.autogather=false;
set hive.stats.column.autogather=false;
insert into table stats2  values (3, "baz");
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
desc formatted stats2;
desc formatted stats2 key;
explain select count(*) from stats2;
explain select count(distinct key) from stats2;

analyze table stats2 compute statistics;
desc formatted stats2;
desc formatted stats2 key;
explain select count(*) from stats2;
explain select min(key) from stats2;

analyze table stats2 compute statistics for columns;
desc formatted stats2;
desc formatted stats2 key;
explain select count(*) from stats2;
explain select min(key) from stats2;


truncate table stats2;
desc formatted stats2;
desc formatted stats2 key;
explain select count(*) from stats2;
explain select count(distinct key) from stats2;

analyze table stats2 compute statistics;
desc formatted stats2;
desc formatted stats2 key;
explain select count(*) from stats2;
explain select min(key) from stats2;

analyze table stats2 compute statistics for columns;
desc formatted stats2;
desc formatted stats2 key;
explain select count(*) from stats2;
explain select min(key) from stats2;

drop table stats2;
