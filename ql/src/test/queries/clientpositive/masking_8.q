--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop table masking_test_n2;	

create table masking_test_n2 as select cast(key as int) as key, value, '12' from src;
	
explain select *, ROW__ID from masking_test_n2;

select *, ROW__ID from masking_test_n2;

explain select * from masking_test_n2;

select * from masking_test_n2;

explain select INPUT__FILE__NAME, *, ROW__ID from masking_test_n2;

select INPUT__FILE__NAME, *, ROW__ID from masking_test_n2;
	
drop table masking_test_n2;

create table masking_test_n2 as select cast(key as int) as key, '12'
'12', '12', '12', '12', '12', '12', '12', '12', '12', '12'
 from src;

explain select ROW__ID, * from masking_test_n2;

select ROW__ID, * from masking_test_n2;

drop table masking_test_n2;

create table masking_test_n2 as select cast(key as int) as key, '12'
'12', '12', '12', '12', '12', INPUT__FILE__NAME as file_name, '12', '12', '12', '12', '12'
 from src;

select INPUT__FILE__NAME, *, ROW__ID from masking_test_n2;
