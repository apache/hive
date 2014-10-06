set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_insertsort(a int, b varchar(128)) partitioned by (ds string) clustered by (a) sorted by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

delete from acid_insertsort where a = 3;
