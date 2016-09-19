set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table umv_basetable (a int, b varchar(256), c decimal(10,2));


create materialized view umv_mat_view as select a, b, c from umv_basetable;

update umv_mat_view set b = 'joe' where b = 'fred';
