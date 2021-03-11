--! qt:dataset:srcpart
--! qt:dataset:src
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.materializedview.rewriting=false;


create table `masking_test_n_mv` stored as orc TBLPROPERTIES ('transactional'='true') as
select cast(key as int) as key, value from src;

explain
create materialized view `masking_test_view_n_mv` as
select key from `masking_test_n_mv`;
create materialized view `masking_test_view_n_mv` as
select key from `masking_test_n_mv`;
describe formatted `masking_test_view_n_mv`;

-- Masking/filtering is added when query is rewritten -> no exact sql text matches -> mv is not used.
explain cbo
select key from `masking_test_n_mv`;


create materialized view `masking_test_view_n_mv_masked` as
select `masking_test_n_mv`.`key` from (SELECT `masking_test_n_mv`.`key`, CAST(reverse(`masking_test_n_mv`.`value`) AS string) AS `value`, `masking_test_n_mv`.`block__offset__inside__file`, `masking_test_n_mv`.`input__file__name`, `masking_test_n_mv`.`row__id`, `masking_test_n_mv`.`row__is__deleted` FROM `default`.`masking_test_n_mv`  WHERE `masking_test_n_mv`.`key` % 2 = 0 and `masking_test_n_mv`.`key` < 10)`masking_test_n_mv`;

-- This will use the MV `masking_test_view_n_mv_masked` because it already has the required masking/filtering for the following query
explain cbo
select key from `masking_test_n_mv`;

select key from `masking_test_n_mv`;

drop materialized view `masking_test_view_n_mv`;
drop materialized view `masking_test_view_n_mv_masked`;
drop table `masking_test_n_mv`;
