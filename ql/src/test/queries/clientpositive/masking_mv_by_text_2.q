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

create materialized view `masking_test_view_n_mv` as
select cast(key as string) col0 from `masking_test_n_mv` union select value col0 from `masking_test_n_mv`;

-- Masking/filtering is added when query is rewritten -> no exact sql text matches -> mv is not used.
explain cbo
select col0 from (select cast(key as string) col0 from `masking_test_n_mv` union select value col0 from `masking_test_n_mv`) sub;


create materialized view `masking_test_view_n_mv_masked` as
select cast(key as string) `col0` from (SELECT `key`, CAST(reverse(value) AS string) AS `value`, BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, ROW__ID, ROW__IS__DELETED FROM `default`.`masking_test_n_mv`  WHERE key % 2 = 0 and key < 10)`masking_test_n_mv` union select `value` `col0` from (SELECT `key`, CAST(reverse(value) AS string) AS `value`, BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, ROW__ID, ROW__IS__DELETED FROM `default`.`masking_test_n_mv`  WHERE key % 2 = 0 and key < 10)`masking_test_n_mv`;

-- This will use the MV `masking_test_view_n_mv_masked` because it already has the required masking/filtering for the following query
explain cbo
select col0 from (select cast(key as string) col0 from `masking_test_n_mv` union select value col0 from `masking_test_n_mv`) sub;

select col0 from (select cast(key as string) col0 from `masking_test_n_mv` union select value col0 from `masking_test_n_mv`) sub;

drop materialized view `masking_test_view_n_mv`;
drop materialized view `masking_test_view_n_mv_masked`;
drop table `masking_test_n_mv`;
