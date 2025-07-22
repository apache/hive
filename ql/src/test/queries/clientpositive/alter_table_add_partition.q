create table mp (a int) partitioned by (b int);

desc formatted mp;

alter table mp add partition (b=1);

desc formatted mp;
desc formatted mp partition (b=1);

insert into mp partition (b=1) values (1);

desc formatted mp;
desc formatted mp partition (b=1);

drop table test_part;
create table test_part(a int) partitioned by (b string);

alter table test_part add partition(b='one');

set hive.exec.default.partition.name=random;

alter table test_part add partition(b='random_access_memory');

show partitions test_part;

alter table test_part add partition(b='partition_random');

show partitions test_part;

drop table test_part;
