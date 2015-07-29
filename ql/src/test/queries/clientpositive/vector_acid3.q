set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.vectorized.execution.enabled=true;

drop table if exists testacid1;

create table testacid1(id int) clustered by (id) into 2 buckets stored as orc tblproperties("transactional"="true");

insert into table testacid1 values (1),(2),(3),(4);

set hive.compute.query.using.stats=false;

set hive.vectorized.execution.enabled;

select count(1) from testacid1;
