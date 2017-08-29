set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop table masking_test;	

create temporary table masking_test as select cast(key as int) as key, value from src;
	
set hive.groupby.position.alias = true;
set hive.cbo.enable=true;

explain select 2017 as a, value from masking_test group by 1, 2;

select 2017 as a, value from masking_test group by 1, 2;

explain
select * from
  masking_test alias01
  left join
  (
      select 2017 as a, value from masking_test group by 1, 2
  ) alias02
  on alias01.key = alias02.a
  left join
  masking_test alias03
on alias01.key = alias03.key;
