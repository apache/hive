-- HIVE-30037: an INSERT with a column list over a masked and row-filtered table; sort keys refer to the select list
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table masking_test_n30037 (key int, value string);
-- the row filter keeps even keys below 10, and value is masked with reverse(value)
insert into masking_test_n30037 values (2, 'bq'), (8, 'hr'), (4, 'ds'), (9, 'it'), (5, 'eu');
create table iwsm_t (a string, b string);

insert into iwsm_t (b, a) select cast(key as string) as k, value from masking_test_n30037 order by k desc limit 1;
select a, b from iwsm_t;
truncate table iwsm_t;
insert into iwsm_t (b, a) select cast(key as string), value from masking_test_n30037 tablesample (5 rows) order by 1 desc limit 1;
select a, b from iwsm_t;
truncate table iwsm_t;
insert into iwsm_t (b, a) select cast(key as string), value from (select key, value from masking_test_n30037 sort by key limit 10) q order by 1 desc limit 1;
select a, b from iwsm_t;
