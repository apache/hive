set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table T1_n74(key string, val string) stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n74;

select * from T1_n74;

create table T2_n45(key string, val string) stored as textfile;

insert overwrite table T2_n45 select * from T1_n74;

select * from T2_n45;

drop table T1_n74;
drop table T2_n45;
