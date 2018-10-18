--! qt:dataset:src
set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table intermediate_n3;

create table intermediate_n3(key int) partitioned by (p int) stored as orc;
insert into table intermediate_n3 partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate_n3 partition(p='456') select distinct key from src where key is not null order by key asc limit 2;
insert into table intermediate_n3 partition(p='457') select distinct key from src where key >= 100 order by key asc limit 2;

drop table multi_partitioned;

create table multi_partitioned (key int, key2 int) partitioned by (p int);

from intermediate_n3
insert into table multi_partitioned partition(p=1) select p, key
insert into table multi_partitioned partition(p=2) select key, p;

select * from multi_partitioned order by key, key2, p;
desc formatted multi_partitioned;

from intermediate_n3
insert overwrite table multi_partitioned partition(p=2) select p, key
insert overwrite table multi_partitioned partition(p=1) select key, p;

select * from multi_partitioned order by key, key2, p;
desc formatted multi_partitioned;

from intermediate_n3
insert into table multi_partitioned partition(p=2) select p, key
insert overwrite table multi_partitioned partition(p=1) select key, p;

select * from multi_partitioned order by key, key2, p;
desc formatted multi_partitioned;

from intermediate_n3
insert into table multi_partitioned partition(p) select p, key, p
insert into table multi_partitioned partition(p=1) select key, p;

select key, key2, p from multi_partitioned order by key, key2, p;
desc formatted multi_partitioned;

from intermediate_n3
insert into table multi_partitioned partition(p) select p, key, 1
insert into table multi_partitioned partition(p=1) select key, p;

select key, key2, p from multi_partitioned order by key, key2, p;
desc formatted multi_partitioned;

drop table multi_partitioned;

drop table intermediate_n3;


