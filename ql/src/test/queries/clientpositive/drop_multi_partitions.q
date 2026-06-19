create database dmp;

create table dmp.mp_n0 (a string) partitioned by (b string, c string);

alter table dmp.mp_n0 add partition (b='1', c='1');
alter table dmp.mp_n0 add partition (b='1', c='2');
alter table dmp.mp_n0 add partition (b='2', c='2');

show partitions dmp.mp_n0;

explain extended alter table dmp.mp_n0 drop partition (b='1');
alter table dmp.mp_n0 drop partition (b='1');

show partitions dmp.mp_n0;

set hive.exec.drop.ignorenonexistent=false;
alter table dmp.mp_n0 drop if exists partition (b='3');

show partitions dmp.mp_n0;

drop table dmp.mp_n0;

drop database dmp;
