set hive.mapred.mode=nonstrict;


explain 
select 'tst1' as key, count(1) as value from src s1
UNION ALL
select key, value from (select 'tst2' as key, count(1) as value from src s2 UNION ALL select 'tst3' as key, count(1) as value from src s3) s4
order by 1;

select 'tst1' as key, count(1) as value from src s1
UNION ALL
select key, value from (select 'tst2' as key, count(1) as value from src s2 UNION ALL select 'tst3' as key, count(1) as value from src s3) s4
order by 1;

drop table src_10;
create table src_10 as select * from src limit 10;

explain 
select key as value, value as key from src_10
UNION ALL
select 'test', value from src_10 s3
order by 2, 1 desc;


select key as value, value as key from src_10
UNION ALL
select 'test', value from src_10 s3
order by 2, 1 desc;

drop table src_10;