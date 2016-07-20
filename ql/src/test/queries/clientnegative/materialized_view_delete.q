set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table dmv_basetable (a int, b varchar(256), c decimal(10,2));


create materialized view dmv_mat_view as select a, b, c from dmv_basetable;

delete from dmv_mat_view where b = 'fred';
