set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

alter table t compact 'middle';

drop table t;

