--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table `masking_test_n5` as select cast(key as int) as key, value from src;

create view `v0` as select * from `masking_test_n5`;

explain
select * from `v0`;

select * from `v0`;

create table `masking_test_subq_n1` as select cast(key as int) as key, value from src;

create view `v1_n9` as select * from `masking_test_subq_n1`;

explain
select * from `v1_n9`
limit 20;

select * from `v1_n9`
limit 20;

create view `masking_test_view` as select key from `v0`;

explain
select key from `masking_test_view`;

select key from `masking_test_view`;

explain
select `v0`.value from `v0` join `masking_test_view` on `v0`.key = `masking_test_view`.key;

select `v0`.value from `v0` join `masking_test_view` on `v0`.key = `masking_test_view`.key;
