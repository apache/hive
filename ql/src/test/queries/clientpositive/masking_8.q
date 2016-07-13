set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop table masking_test;	

create table masking_test as select cast(key as int) as key, value, '12' from src;
	
explain select *, ROW__ID from masking_test;

select *, ROW__ID from masking_test;

explain select * from masking_test;

select * from masking_test;

explain select INPUT__FILE__NAME, *, ROW__ID from masking_test;

select INPUT__FILE__NAME, *, ROW__ID from masking_test;
	
drop table masking_test;

create table masking_test as select cast(key as int) as key, '12'
'12', '12', '12', '12', '12', '12', '12', '12', '12', '12'
 from src;

explain select ROW__ID, * from masking_test;

select ROW__ID, * from masking_test;

drop table masking_test;

create table masking_test as select cast(key as int) as key, '12'
'12', '12', '12', '12', '12', INPUT__FILE__NAME, '12', '12', '12', '12', '12'
 from src;

select INPUT__FILE__NAME, *, ROW__ID from masking_test;
