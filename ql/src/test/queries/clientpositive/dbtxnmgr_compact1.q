-- Mask the enqueue time which is based on current time
--! qt:replace:/(initiated\s+---\s+---\s+)[0-9]*(\s+---)/$1#Masked#$2/
--! qt:replace:/(---\s+)[a-zA-Z0-9\-]+(\s+manual)/$1#Masked#$2/

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table T1_n153(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

create table T2_n153(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

alter table T1_n153 compact 'major';

alter table T2_n153 compact 'minor';

explain alter table T1_n153 compact 'MAJOR' pool 'test';

show compactions;

drop table T1_n153;

drop table T2_n153;
