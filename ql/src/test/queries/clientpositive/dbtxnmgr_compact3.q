set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create database D1;

use D1;

create table T1_n71(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

create table T2_n71(key string, val string) clustered by (val) into 2 buckets stored as ORC TBLPROPERTIES ('transactional'='true');

alter table T1_n71 compact 'major';

alter table T2_n71 compact 'minor';

drop table T1_n71;

drop table T2_n71;