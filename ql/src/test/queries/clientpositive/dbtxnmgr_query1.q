set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table T1_n20(key string, val string) stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n20;

select * from T1_n20;

create table T2_n12(key string, val string) stored as textfile;

insert into table T2_n12 select * from T1_n20;

select * from T2_n12;

drop table T1_n20;
drop table T2_n12;
