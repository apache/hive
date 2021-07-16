set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.create.as.external.legacy=true;
create managed table test as select 1;
show create table test;
