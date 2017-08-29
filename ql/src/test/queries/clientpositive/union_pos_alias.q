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


drop view v;
create view v as select key as k from src intersect all select key as k1 from src;
desc formatted v;

set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table masking_test as select cast(key as int) as key, value from src;

explain
select * from masking_test  union all select * from masking_test ;
select * from masking_test  union all select * from masking_test ;

explain
select key as k1, value as v1 from masking_test where key > 0 intersect all select key as k2, value as v2 from masking_test where key > 0;
select key as k1, value as v1 from masking_test where key > 0 intersect all select key as k2, value as v2 from masking_test where key > 0;
