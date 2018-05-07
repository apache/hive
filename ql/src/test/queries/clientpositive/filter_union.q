--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.optimize.metadataonly=true;

explain extended 
select key, c, m from
(
select key, c, 1 as m from (select key, count(key) as c from src group by key)s1
union all
select key, c, 2 as m from (select key, count(key) as c from src group by key)s2
union all
select key, c, 3 as m from (select key, count(key) as c from src group by key)s3
union all
select key, c, 4 as m from (select key, count(key) as c from src group by key)s4
)sub
where m >2;


explain 
select key, c, m from
(
select key, c, 1 as m from (select key, count(key) as c from src group by key)s1
union all
select key, c, 2 as m from (select key, count(key) as c from src group by key)s2
union all
select key, c, 3 as m from (select key, count(key) as c from src group by key)s3
union all
select key, c, 4 as m from (select key, count(key) as c from src group by key)s4
)sub
where m = 1;

explain 
select key, c, m from
(
select key, c, 1 as m from (select key, count(key) as c from src group by key)s1
union all
select key, c, 2 as m from (select key, count(key) as c from src group by key)s2
union all
select key, c, 3 as m from (select key, count(key) as c from src group by key)s3
union all
select key, c, 4 as m from (select key, count(key) as c from src group by key)s4
)sub
where m = 4;


explain 
select key, c, m from
(
select key, c, 1 as m from (select key, count(key) as c from src group by key)s1
union all
select key, c, 2 as m from (select key, count(key) as c from src group by key)s2
union all
select key, c, 3 as m from (select key, count(key) as c from src group by key)s3
union all
select key, c, 4 as m from (select key, count(key) as c from src group by key)s4
)sub
where m = 5;
