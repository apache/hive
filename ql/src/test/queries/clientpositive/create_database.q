set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
create database testdb location '/tmp/testdb.db';
create table testdb.test as select 1;
