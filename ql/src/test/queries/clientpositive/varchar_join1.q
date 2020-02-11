set hive.mapred.mode=nonstrict;
drop table varchar_join1_vc1_n0;
drop table varchar_join1_vc2_n0;
drop table varchar_join1_str_n0;

create table  varchar_join1_vc1_n0 (
  c1 int,
  c2 varchar(10)
);

create table  varchar_join1_vc2_n0 (
  c1 int,
  c2 varchar(20)
);

create table  varchar_join1_str_n0 (
  c1 int,
  c2 string
);

load data local inpath '../../data/files/vc1.txt' into table varchar_join1_vc1_n0;
load data local inpath '../../data/files/vc1.txt' into table varchar_join1_vc2_n0;
load data local inpath '../../data/files/vc1.txt' into table varchar_join1_str_n0;

-- Join varchar with same length varchar
select * from varchar_join1_vc1_n0 a join varchar_join1_vc1_n0 b on (a.c2 = b.c2) order by a.c1;

-- Join varchar with different length varchar
select * from varchar_join1_vc1_n0 a join varchar_join1_vc2_n0 b on (a.c2 = b.c2) order by a.c1;

-- Join varchar with string
select * from varchar_join1_vc1_n0 a join varchar_join1_str_n0 b on (a.c2 = b.c2) order by a.c1;

drop table varchar_join1_vc1_n0;
drop table varchar_join1_vc2_n0;
drop table varchar_join1_str_n0;
