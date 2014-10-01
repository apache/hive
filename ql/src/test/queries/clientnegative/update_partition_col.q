set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table foo(a int, b varchar(128)) partitioned by (ds string) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

update foo set ds = 'fred';
