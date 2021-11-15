create database dmp;

create table dmp.mp (a string) partitioned by (b string, c string) location '/tmp/dmp_mp';

alter table dmp.mp add partition (b='1', c='1');

show partitions dmp.mp;

dfs -rm -R /tmp/dmp_mp/b=1;

explain extended alter table dmp.mp drop partition (b='1');
alter table dmp.mp drop partition (b='1');

show partitions dmp.mp;

drop table dmp.mp;

create table dmp.delete_parent_path (c1 int) partitioned by (year string, month string, day string) location '/tmp/delete_parent_path';

alter table dmp.delete_parent_path add partition (year='2019', month='07', day='01');

show partitions dmp.delete_parent_path;

dfs -rm -r /tmp/delete_parent_path/year=2019/month=07;

alter table dmp.delete_parent_path drop partition (year='2019', month='07', day='01');

show partitions dmp.delete_parent_path;

drop table dmp.delete_parent_path;

drop database dmp;
