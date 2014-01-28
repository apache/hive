-- top level
explain
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

explain
select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) limit 10
union all
select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) limit 10;

select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) limit 10
union all
select s1.key as k, s2.value as v from src s1 join src s2 on (s1.key = s2.key) limit 10;

-- ctas
explain
create table union_top as
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

create table union_top as
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

select * from union_top;

truncate table union_top;

-- insert into
explain
insert into table union_top
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

insert into table union_top
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

select * from union_top;

explain
insert overwrite table union_top
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

insert overwrite table union_top
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

select * from union_top;

-- create view
explain
create view union_top_view as
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

create view union_top_view as
select key, 0 as value from src where key % 3 == 0 limit 3
union all
select key, 1 as value from src where key % 3 == 1 limit 3
union all
select key, 2 as value from src where key % 3 == 2 limit 3;

select * from union_top_view;

drop table union_top;
drop view union_top_view;
