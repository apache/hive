--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table masking_test_n11 as select cast(key as int) as key, value from src;
create table masking_test_subq_n2 as select cast(key as int) as key, value from src;


explain
with q1 as ( select key from q2 where key = '5'),
q2 as ( select key from src where key = '5')
select * from (select key from q1) a;


--should mask masking_test_n11

explain
with q1 as ( select * from masking_test_n11 where key = '5')
select * from q1;

--should not mask masking_test_subq_n2

explain
with masking_test_subq_n2 as ( select * from masking_test_n11 where key = '5')
select * from masking_test_subq_n2;

--should mask masking_test_subq_n2

explain
with q1 as ( select * from masking_test_n11 where key = '5')
select * from masking_test_subq_n2;