set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition=true;
set hive.vectorized.execution.enabled=true;
set hive.create.as.insert.only=true;

drop table if exists studentparttab30k;
create table studentparttab30k (name string) row format delimited fields terminated by '\\t' stored as textfile;
insert into studentparttab30k values('a');

drop table if exists multi_insert_1;
create table multi_insert_1 (name string) row format delimited fields terminated by '\\t' stored as textfile;

set hive.test.rollbacktxn=true;

insert overwrite table multi_insert_1 select name FROM studentparttab30k;

set hive.test.rollbacktxn=false;
select * from multi_insert_1;
