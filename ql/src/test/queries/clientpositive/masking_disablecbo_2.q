--! qt:dataset:src
-- SORT_QUERY_RESULTS

set hive.cbo.enable=false;
set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create view masking_test_n13 as select cast(key as int) as key, value from src;

explain select * from masking_test_n13;
select * from masking_test_n13;

explain select * from masking_test_n13 where key > 0;
select * from masking_test_n13 where key > 0;

explain select * from src a join masking_test_n13 b on a.key = b.value where b.key > 0;

explain select a.*, b.key from masking_test_n13 a join masking_test_n13 b on a.key = b.value where b.key > 0;

explain select * from masking_test_n13 a union select b.* from masking_test_n13 b where b.key > 0;

