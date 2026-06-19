-- Test multi inserting to the same partition
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table source(p int, key int,value string);
insert into source(p, key, value) values (101,42,'string42');

create table stats_part(key int,value string) partitioned by (p int) stored as orc tblproperties ("transactional"="true");

from source
insert into stats_part select key, value, p
insert into stats_part select key, value, p;

select p, key, value from stats_part;
desc formatted stats_part;

set hive.compute.query.using.stats=true;
select count(*) from stats_part;

set hive.compute.query.using.stats=false;
select count(*) from stats_part;
