--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- This test covers HIVE-2332
-- SORT_QUERY_RESULTS

create table t1_n60 (int1_n60 int, int2 int, str1 string, str2 string);

set hive.optimize.reducededuplication=false;
--disabled RS-dedup for keeping intention of test

insert into table t1_n60 select cast(key as int), cast(key as int), value, value from src where key < 6;
explain select Q1.int1_n60, sum(distinct Q1.int1_n60) from (select * from t1_n60 order by int1_n60) Q1 group by Q1.int1_n60;
explain select int1_n60, sum(distinct int1_n60) from t1_n60 group by int1_n60;

select Q1.int1_n60, sum(distinct Q1.int1_n60) from (select * from t1_n60 order by int1_n60) Q1 group by Q1.int1_n60;
select int1_n60, sum(distinct int1_n60) from t1_n60 group by int1_n60;

drop table t1_n60;
