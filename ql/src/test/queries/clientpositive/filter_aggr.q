--! qt:dataset:src
set hive.mapred.mode=nonstrict;

explain extended 
select key, c, m from
(
select key, c, 1 as m from (select key, count(key) as c from src group by key)s1
union all
select key, c, 2 as m from (select key, count(key) as c from src group by key)s2
)sub
where m = 1;

select key, c, m from
(
select key, c, 1 as m from (select key, count(key) as c from src group by key)s1 
union all 
select key, c, 2 as m from (select key, count(key) as c from src group by key)s2
)sub
where m = 1;
