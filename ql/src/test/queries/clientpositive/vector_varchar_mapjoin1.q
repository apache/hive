set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;

drop table if exists varchar_join1_vc1;
drop table if exists varchar_join1_vc2;
drop table if exists varchar_join1_str;
drop table if exists varchar_join1_vc1_orc;
drop table if exists varchar_join1_vc2_orc;
drop table if exists varchar_join1_str_orc;

create table  varchar_join1_vc1 (
  c1 int,
  c2 varchar(10)
);

create table  varchar_join1_vc2 (
  c1 int,
  c2 varchar(20)
);

create table  varchar_join1_str (
  c1 int,
  c2 string
);

load data local inpath '../../data/files/vc1.txt' into table varchar_join1_vc1;
load data local inpath '../../data/files/vc1.txt' into table varchar_join1_vc2;
load data local inpath '../../data/files/vc1.txt' into table varchar_join1_str;

create table varchar_join1_vc1_orc stored as orc as select * from varchar_join1_vc1;
create table varchar_join1_vc2_orc stored as orc as select * from varchar_join1_vc2;
create table varchar_join1_str_orc stored as orc as select * from varchar_join1_str;

-- Join varchar with same length varchar
explain vectorization select * from varchar_join1_vc1_orc a join varchar_join1_vc1_orc b on (a.c2 = b.c2) order by a.c1;
select * from varchar_join1_vc1_orc a join varchar_join1_vc1_orc b on (a.c2 = b.c2) order by a.c1;

-- Join varchar with different length varchar
explain vectorization select * from varchar_join1_vc1_orc a join varchar_join1_vc2_orc b on (a.c2 = b.c2) order by a.c1;
select * from varchar_join1_vc1_orc a join varchar_join1_vc2_orc b on (a.c2 = b.c2) order by a.c1;

-- Join varchar with string
explain vectorization select * from varchar_join1_vc1_orc a join varchar_join1_str_orc b on (a.c2 = b.c2) order by a.c1;
select * from varchar_join1_vc1_orc a join varchar_join1_str_orc b on (a.c2 = b.c2) order by a.c1;

drop table varchar_join1_vc1;
drop table varchar_join1_vc2;
drop table varchar_join1_str;
drop table varchar_join1_vc1_orc;
drop table varchar_join1_vc2_orc;
drop table varchar_join1_str_orc;
