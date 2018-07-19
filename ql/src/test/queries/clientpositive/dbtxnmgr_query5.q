set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create database foo;

use foo;

create table T1_n40(key string, val string) partitioned by (ds string) stored as textfile;

alter table T1_n40 add partition (ds='today');

create view V1_n3 as select key from T1_n40;

show tables;

describe T1_n40;

drop view V1_n3;

drop table T1_n40;

show databases;

drop database foo;
