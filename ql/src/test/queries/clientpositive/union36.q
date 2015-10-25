set hive.cbo.enable=false;

select (x/sum(x) over())  as y from(select cast(1 as decimal(10,0))  as x from (select * from src limit 2)s1 union all select cast(1 as decimal(10,0)) x from (select * from src limit 2) s2 union all select '100000000' x from (select * from src limit 2) s3)u order by y;

select (x/sum(x) over()) as y from(select cast(1 as decimal(10,0))  as x from (select * from src limit 2)s1 union all select cast(1 as decimal(10,0)) x from (select * from src limit 2) s2 union all select cast (null as string) x from (select * from src limit 2) s3)u order by y;





