--! qt:dataset:src

set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

set hive.query.results.cache.enabled=true;
set hive.query.results.cache.nontransactional.tables.enabled=true;

create table masking_test_n7 as select cast(key as int) as key, value from src;

explain
select key, count(*) from masking_test_n7 group by key;
select key, count(*) from masking_test_n7 group by key;

-- It will not use the cache as it is masked
-- TODO: We should use the cache
explain
select key, count(*) from masking_test_n7 group by key;
select key, count(*) from masking_test_n7 group by key;

