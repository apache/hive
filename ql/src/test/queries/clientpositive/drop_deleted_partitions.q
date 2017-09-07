create database dmp;

create table dmp.mp (a string) partitioned by (b string, c string) location '/tmp/dmp_mp';

alter table dmp.mp add partition (b='1', c='1');

show partitions dmp.mp;

dfs -rm -R /tmp/dmp_mp/b=1;

explain extended alter table dmp.mp drop partition (b='1');
alter table dmp.mp drop partition (b='1');

show partitions dmp.mp;

drop table dmp.mp;

drop database dmp;
