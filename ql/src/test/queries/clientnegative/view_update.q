set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table uv_basetable (a int, b varchar(256), c decimal(10,2));

create view uv_view as select a, b, c from uv_basetable;

update uv_view set b = 'joe' where b = 'fred';
