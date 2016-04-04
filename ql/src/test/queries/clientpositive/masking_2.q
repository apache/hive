set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create view masking_test as select cast(key as int) as key, value from src;

explain select * from masking_test;
select * from masking_test;

explain select * from masking_test where key > 0;
select * from masking_test where key > 0;

explain select * from src a join masking_test b on a.key = b.value where b.key > 0;

explain select a.*, b.key from masking_test a join masking_test b on a.key = b.value where b.key > 0;

explain select * from masking_test a union select b.* from masking_test b where b.key > 0;

