set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table dv_basetable (a int, b varchar(256), c decimal(10,2));

create view dv_view as select a, b, c from dv_basetable;

delete from dv_view where b = 'fred';
