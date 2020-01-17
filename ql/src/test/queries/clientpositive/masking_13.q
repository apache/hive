--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table masking_test as select cast(key as int) as key, value from src;

explain select * from masking_test;
select * from masking_test;

create table new_masking_test_nx as
select * from masking_test;
select * from new_masking_test_nx;

create view `masking_test_view` as select key from `masking_test`;

explain
select key from `masking_test_view`;
select key from `masking_test_view`;

create table `my_table_masked` (key int);
insert into `my_table_masked` select key from `masking_test_view`;
select * from `my_table_masked`;

create table new_masking_test_nx_2 as
select * from masking_test_view;

select * from new_masking_test_nx_2;
