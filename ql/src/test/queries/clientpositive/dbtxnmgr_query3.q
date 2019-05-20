set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table T1_n111(key string, val string) stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n111;

select * from T1_n111;

create table T2_n67(key string, val string) partitioned by (pval string) stored as textfile;

insert into table T2_n67 partition (pval = '1') select * from T1_n111;

select * from T2_n67;

insert overwrite table T2_n67 partition (pval = '1') select * from T1_n111;

select * from T2_n67;

drop table T1_n111;
drop table T2_n67;
