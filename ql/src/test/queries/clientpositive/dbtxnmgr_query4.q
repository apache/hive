set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

create table T1_n163(key string, val string) stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n163;

select * from T1_n163;

create table T2_n95(key string) partitioned by (val string) stored as textfile;

insert overwrite table T2_n95 partition (val) select key, val from T1_n163;

select * from T2_n95;

drop table T1_n163;
drop table T2_n95;
