--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table masking_test_n6 as select cast(key as int) as key, value from src;

explain select * from masking_test_n6 tablesample (10 rows);
select * from masking_test_n6 tablesample (10 rows);

explain
select * from masking_test_n6 tablesample(1 percent);
select * from masking_test_n6 tablesample(1 percent);

drop table masking_test_n6;

CREATE TABLE masking_test_n6(key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS;

insert overwrite table masking_test_n6
select * from src;

explain
select * from masking_test_n6 tablesample (bucket 1 out of 2) s;
select * from masking_test_n6 tablesample (bucket 1 out of 2) s;
