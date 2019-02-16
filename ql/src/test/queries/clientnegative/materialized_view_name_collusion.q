set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table mvnc_basetable (a int, b varchar(256), c decimal(10,2));


create materialized view mvnc_mat_view disable rewrite as select a, b, c from mvnc_basetable;

create materialized view mvnc_mat_view disable rewrite as select a, b, c from mvnc_basetable;
