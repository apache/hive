set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop table masking_test;	

create temporary table masking_test as select cast(key as int) as key, value from src;
	
explain select ROW__ID from masking_test;
select ROW__ID from masking_test;
