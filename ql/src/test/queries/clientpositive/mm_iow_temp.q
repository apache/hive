--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.create.as.insert.only=true;

create temporary table temptable1 (
  key string,
  value string
);

insert overwrite table temptable1 select * from src; 

show create table temptable1;
select * from temptable1 order by key limit 10;
