--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop view masking_test_n14;	

create view masking_test_n14 as select cast(key as int) as key, value, '12', ROW__ID from src;
	
explain select * from masking_test_n14;
	
select * from masking_test_n14;
	
explain select * from masking_test_n14 where key > 0;
	
select * from masking_test_n14 where key > 0;

drop view masking_test_n14;

create view masking_test_n14 as select cast(key as int) as key, '12', ROW__ID,
'12', '12', '12', '12', '12', '12', '12', '12', '12', '12'
 from src;

explain select * from masking_test_n14;

select * from masking_test_n14;

explain select * from masking_test_n14 where key > 0;

select * from masking_test_n14 where key > 0;
