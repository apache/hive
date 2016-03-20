set hive.cbo.enable=false;
set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table masking_test_subq as select cast(key as int) as key, value from src;

explain select * from masking_test_subq;
select * from masking_test_subq;

explain select * from masking_test_subq where key > 0;
select * from masking_test_subq where key > 0;

explain select key from masking_test_subq where key > 0;
select key from masking_test_subq where key > 0;

explain select value from masking_test_subq where key > 0;
select value from masking_test_subq where key > 0;

explain select * from masking_test_subq join srcpart on (masking_test_subq.key = srcpart.key);
select * from masking_test_subq join srcpart on (masking_test_subq.key = srcpart.key);

explain select * from default.masking_test_subq where key > 0;
select * from default.masking_test_subq where key > 0;

explain select * from masking_test_subq where masking_test_subq.key > 0;
select * from masking_test_subq where masking_test_subq.key > 0;

explain select key, value from (select key, value from (select key, upper(value) as value from src where key > 0) t where key < 10) t2 where key % 2 = 0;
