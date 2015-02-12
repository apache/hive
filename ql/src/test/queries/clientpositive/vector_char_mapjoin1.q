SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;

drop table if exists char_join1_vc1;
drop table if exists char_join1_vc2;
drop table if exists char_join1_str;
drop table if exists char_join1_vc1_orc;
drop table if exists char_join1_vc2_orc;
drop table if exists char_join1_str_orc;

create table  char_join1_vc1 (
  c1 int,
  c2 char(10)
);

create table  char_join1_vc2 (
  c1 int,
  c2 char(20)
);

create table  char_join1_str (
  c1 int,
  c2 string
);

load data local inpath '../../data/files/vc1.txt' into table char_join1_vc1;
load data local inpath '../../data/files/vc1.txt' into table char_join1_vc2;
load data local inpath '../../data/files/vc1.txt' into table char_join1_str;

create table char_join1_vc1_orc stored as orc as select * from char_join1_vc1;
create table char_join1_vc2_orc stored as orc as select * from char_join1_vc2;
create table char_join1_str_orc stored as orc as select * from char_join1_str;

-- Join char with same length char
explain select * from char_join1_vc1_orc a join char_join1_vc1_orc b on (a.c2 = b.c2) order by a.c1;
select * from char_join1_vc1_orc a join char_join1_vc1_orc b on (a.c2 = b.c2) order by a.c1;

-- Join char with different length char
explain select * from char_join1_vc1_orc a join char_join1_vc2_orc b on (a.c2 = b.c2) order by a.c1;
select * from char_join1_vc1_orc a join char_join1_vc2_orc b on (a.c2 = b.c2) order by a.c1;

-- Join char with string
explain select * from char_join1_vc1_orc a join char_join1_str_orc b on (a.c2 = b.c2) order by a.c1;
select * from char_join1_vc1_orc a join char_join1_str_orc b on (a.c2 = b.c2) order by a.c1;

drop table char_join1_vc1;
drop table char_join1_vc2;
drop table char_join1_str;
drop table char_join1_vc1_orc;
drop table char_join1_vc2_orc;
drop table char_join1_str_orc;
