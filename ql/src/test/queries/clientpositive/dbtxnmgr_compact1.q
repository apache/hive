set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table T1_n153(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

alter table T1_n153 compact 'major';

alter table T1_n153 compact 'minor';

drop table T1_n153;
