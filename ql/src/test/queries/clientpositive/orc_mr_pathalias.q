create database if not exists test;
drop table if exists test.test_orc_src;
drop table if exists test.test_orc_src2;
create table test.test_orc_src (a int, b int, c int) stored as orc;
create table test.test_orc_src2 (a int, b int, d int) stored as orc;
insert overwrite table test.test_orc_src select 1,2,3 from src limit 1;
insert overwrite table test.test_orc_src2 select 1,2,4 from src limit 1;

set hive.auto.convert.join = false;

select
    tb.c
from test.test_orc_src tb
join (
    select * from test.test_orc_src2
) tm
on tb.a = tm.a
where tb.b = 2;
