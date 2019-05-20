--! qt:dataset:src
--! qt:dataset:alltypesorc
explain
with q1 as ( select key from src where key = '5')
select *
from q1
;

with q1 as ( select key from src where key = '5')
select *
from q1
;

-- in subquery
explain
with q1 as ( select key from src where key = '5')
select * from (select key from q1) a;

with q1 as ( select key from src where key = '5')
select * from (select key from q1) a;

-- chaining
explain
with q1 as ( select key from q2 where key = '5'),
q2 as ( select key from src where key = '5')
select * from (select key from q1) a;

with q1 as ( select key from q2 where key = '5'),
q2 as ( select key from src where key = '5')
select * from (select key from q1) a;

with q1 as (select * from alltypesorc)
    select s1.key, s1.value
   from src s1
   where key > 3
   and s1.value in (select q1.cstring1
   from q1
   where cint > 900);

with q1 as (select * from src)
    select key, value,
            max(value) over (partition by key)
               from q1;

with q1 as (select * from alltypesorc)
           from q1
           select cint, cstring1, avg(csmallint)
           group by cint, cstring1 with rollup;
--standard rollup syntax
with q1 as (select * from alltypesorc)
           from q1
           select cint, cstring1, avg(csmallint)
           group by rollup (cint, cstring1);

drop table if exists cte9_t1;
create table cte9_t1 as
        with q1 as (select cint, cstring1 from alltypesorc where cint > 70)
        select * from q1;

drop table if exists cte10_t1;
create table cte10_t1 as
    with q1 as (select cint, cstring1 from alltypesorc where cint > 70)
               select * from q1;
with q1 as (select cint , cstring1 from alltypesorc where age < 50)
                           select * from cte10_t1;
