set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create database D1;

alter database D1 set dbproperties('test'='yesthisis');

drop database D1;

create table T1_n50(key string, val string) stored as textfile;

create table T2_n31 like T1_n50;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n50;

select * from T1_n50;

create table T3_n11 as select * from T1_n50;

create table T4_n4 (key char(10), val decimal(5,2), b int)
    partitioned by (ds string)
    clustered by (b) into 10 buckets
    stored as orc;

alter table T3_n11 rename to newT3_n11;

alter table T2_n31 set tblproperties ('test'='thisisatest');

alter table T2_n31 set serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde';
alter table T2_n31 set serdeproperties ('test'='thisisatest');

alter table T2_n31 clustered by (key) into 32 buckets;

alter table T4_n4 add partition (ds='today'); 

alter table T4_n4 partition (ds='today') rename to partition(ds='yesterday');

alter table T4_n4 drop partition (ds='yesterday');

alter table T4_n4 add partition (ds='tomorrow'); 

create table T5_n1 (a string, b int);
alter table T5_n1 set fileformat orc;

create table T7_n2 (a string, b int);
alter table T7_n2 set location 'file:///tmp';
alter table T4_n4 partition (ds='tomorrow') set location 'file:///tmp';

alter table T2_n31 touch;
alter table T4_n4 touch partition (ds='tomorrow');

create view V1_n5 as select key from T1_n50;
alter view V1_n5 set tblproperties ('test'='thisisatest');
drop view V1_n5;



drop table T1_n50;
drop table T2_n31;
drop table newT3_n11;
