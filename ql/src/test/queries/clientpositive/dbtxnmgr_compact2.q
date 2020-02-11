set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table T1_n105(key string, val string) partitioned by (ds string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

alter table T1_n105 add partition (ds = 'today');
alter table T1_n105 add partition (ds = 'yesterday');

alter table T1_n105 partition (ds = 'today') compact 'major';

explain alter table T1_n105 partition (ds = 'yesterday') compact 'minor';
alter table T1_n105 partition (ds = 'yesterday') compact 'minor';

drop table T1_n105;
