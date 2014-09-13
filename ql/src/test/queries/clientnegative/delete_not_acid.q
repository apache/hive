set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;

create table foo(a int, b varchar(128)) clustered by (a) into 1 buckets stored as orc;

delete from foo;
