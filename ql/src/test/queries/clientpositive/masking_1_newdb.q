set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create database newdb;

use newdb;

create table masking_test as select cast(key as int) as key, value from default.src;

use default;

explain select * from newdb.masking_test;
select * from newdb.masking_test;

explain select * from newdb.masking_test where key > 0;
select * from newdb.masking_test where key > 0;

