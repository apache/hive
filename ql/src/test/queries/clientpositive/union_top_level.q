set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

-- top level
explain
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

explain
select * from (select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) order by k limit 10)a
union all
select * from (select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) order by k limit 10)b;

select * from (select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) order by k limit 10)a
union all
select * from (select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) order by k limit 10)b;

-- ctas
explain
create table union_top as
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

create table union_top as
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

select * from union_top;

truncate table union_top;

-- insert into
explain
insert into table union_top
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

insert into table union_top
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

select * from union_top;

explain
insert overwrite table union_top
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

insert overwrite table union_top
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

select * from union_top;

-- create view
explain
create view union_top_view as
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

create view union_top_view as
select * from (select key, 0 as value from src where key % 3 == 0 limit 3)a
union all
select * from (select key, 1 as value from src where key % 3 == 1 limit 3)b
union all
select * from (select key, 2 as value from src where key % 3 == 2 limit 3)c;

select * from union_top_view;

drop table union_top;
drop view union_top_view;
