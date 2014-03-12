set hive.auto.convert.join=true;

explain 
select s1.key as key, s1.value as value from src s1 join src s3 on s1.key=s3.key
UNION  ALL  
select s2.key as key, s2.value as value from src s2;

select s1.key as key, s1.value as value from src s1 join src s3 on s1.key=s3.key
UNION  ALL  
select s2.key as key, s2.value as value from src s2;

set hive.auto.convert.join=false;

explain
with u as (select * from src union all select * from src)
select count(*) from (select u1.key as k1, u2.key as k2 from
u as u1 join u as u2 on (u1.key = u2.key)) a;

with u as (select * from src union all select * from src)
select count(*) from (select u1.key as k1, u2.key as k2 from
u as u1 join u as u2 on (u1.key = u2.key)) a;
