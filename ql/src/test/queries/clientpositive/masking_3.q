--! qt:dataset:srcpart
--! qt:dataset:src
-- SORT_QUERY_RESULTS

set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table masking_test_subq_n3 as select cast(key as int) as key, value from src;

explain cbo select * from masking_test_subq_n3;
explain select * from masking_test_subq_n3;
select * from masking_test_subq_n3;

explain cbo select * from masking_test_subq_n3 where key > 0;
explain select * from masking_test_subq_n3 where key > 0;
select * from masking_test_subq_n3 where key > 0;

explain cbo select key from masking_test_subq_n3 where key > 0;
explain select key from masking_test_subq_n3 where key > 0;
select key from masking_test_subq_n3 where key > 0;

explain cbo select value from masking_test_subq_n3 where key > 0;
explain select value from masking_test_subq_n3 where key > 0;
select value from masking_test_subq_n3 where key > 0;

explain cbo select * from masking_test_subq_n3 join srcpart on (masking_test_subq_n3.key = srcpart.key);
explain select * from masking_test_subq_n3 join srcpart on (masking_test_subq_n3.key = srcpart.key);
select * from masking_test_subq_n3 join srcpart on (masking_test_subq_n3.key = srcpart.key);

explain cbo select * from default.masking_test_subq_n3 where key > 0;
explain select * from default.masking_test_subq_n3 where key > 0;
select * from default.masking_test_subq_n3 where key > 0;

explain cbo select * from masking_test_subq_n3 where masking_test_subq_n3.key > 0;
explain select * from masking_test_subq_n3 where masking_test_subq_n3.key > 0;
select * from masking_test_subq_n3 where masking_test_subq_n3.key > 0;

explain select key, value from (select key, value from (select key, upper(value) as value from src where key > 0) t where key < 10) t2 where key % 2 = 0;
