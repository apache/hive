set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop view masking_test;	

create view masking_test as select cast(key as int) as key, value, '12', ROW__ID from src;
	
explain select * from masking_test;
	
select * from masking_test;
	
explain select * from masking_test where key > 0;
	
select * from masking_test where key > 0;

drop view masking_test;

create view masking_test as select cast(key as int) as key, '12', ROW__ID,
'12', '12', '12', '12', '12', '12', '12', '12', '12', '12'
 from src;

explain select * from masking_test;

select * from masking_test;

explain select * from masking_test where key > 0;

select * from masking_test where key > 0;
