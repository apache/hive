-- This test covers HIVE-2322

create table t1 (int1 int, int2 int, str1 string, str2 string);

insert into table t1 select cast(key as int), cast(key as int), value, value from src where key < 6;
explain select Q1.int1, sum(distinct Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
explain select int1, sum(distinct int1) from t1 group by int1;

select Q1.int1, sum(distinct Q1.int1) from (select * from t1 order by int1) Q1 group by Q1.int1;
select int1, sum(distinct int1) from t1 group by int1;

drop table t1;
