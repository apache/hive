--! qt:dataset:srcpart
--! qt:dataset:src
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table `masking_test_n_mv` stored as orc TBLPROPERTIES ('transactional'='true') as
select cast(key as int) as key, value from src;

explain
create materialized view `masking_test_view_n_mv` as
select key from `masking_test_n_mv`;
create materialized view `masking_test_view_n_mv` as
select key from `masking_test_n_mv`;

select key from `masking_test_view_n_mv`;
