
set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

set hive.query.results.cache.enabled=true;

create table masking_test as select cast(key as int) as key, value from src;

explain
select key, count(*) from masking_test group by key;
select key, count(*) from masking_test group by key;

-- This time we should use the cache
explain
select key, count(*) from masking_test group by key;
select key, count(*) from masking_test group by key;

