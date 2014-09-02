create database dmp;

create table dmp.mp (a string) partitioned by (b string, c string);

alter table dmp.mp add partition (b='1', c='1');
alter table dmp.mp add partition (b='1', c='2');
alter table dmp.mp add partition (b='2', c='2');

show partitions dmp.mp;

explain extended alter table dmp.mp drop partition (b='1');
alter table dmp.mp drop partition (b='1');

show partitions dmp.mp;

set hive.exec.drop.ignorenonexistent=false;
alter table dmp.mp drop if exists partition (b='3');

show partitions dmp.mp;

drop table dmp.mp;

drop database dmp;
