--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=false;

-- SORT_QUERY_RESULTS
select (x/sum(x) over())  as y from(select cast(1 as decimal(10,0))  as x from (select * from src limit 2)s1 union all select cast(1 as decimal(10,0)) x from (select * from src limit 2) s2 union all select cast('100000000' as decimal(10,0)) x from (select * from src limit 2) s3)u;

select (x/sum(x) over()) as y from(select cast(1 as decimal(10,0))  as x from (select * from src limit 2)s1 union all select cast(1 as decimal(10,0)) x from (select * from src limit 2) s2 union all select cast (null as decimal(10,0)) x from (select * from src limit 2) s3)u;





