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
describe formatted `masking_test_view_n_mv`;

-- This will use the MV and will apply masking/filtering on top
-- of the source table, not any policy that applies on the MV
explain
select key from `masking_test_n_mv`;
select key from `masking_test_n_mv`;

create materialized view `masking_test_view_n_mv_3` as
select value, sum(key) from `masking_test_n_mv` group by value;

-- Rewriting triggered from `masking_test_view_n_mv`
explain
select key from `masking_test_n_mv` group by key;
select key from `masking_test_n_mv` group by key;

-- Rewriting cannot be triggered
explain
select value from `masking_test_n_mv` group by value;
select value from `masking_test_n_mv` group by value;

-- Rewriting cannot be triggered
explain
select value, sum(key) from `masking_test_n_mv` group by value;
select value, sum(key) from `masking_test_n_mv` group by value;

create materialized view `masking_test_view_n_mv_4` as
select key, value from `masking_test_n_mv`;

-- Rewriting triggered from `masking_test_view_n_mv_4`
explain
select value from `masking_test_n_mv` group by value;
select value from `masking_test_n_mv` group by value;

-- Rewriting triggered from `masking_test_view_n_mv_4`
explain
select value, sum(key) from `masking_test_n_mv` group by value;
select value, sum(key) from `masking_test_n_mv` group by value;


create table `srcTnx` stored as orc TBLPROPERTIES ('transactional'='true') as
select cast(key as int) as key, value from src;

explain
create materialized view `masking_test_view_n_mv_2` as
select key from `srcTnx`;
create materialized view `masking_test_view_n_mv_2` as
select key from `srcTnx`;
describe formatted `masking_test_view_n_mv_2`;

-- This is allowed because source tables do not use a masking
-- or filtering. Policy is not applied on MV as expected
explain
select key from `masking_test_view_n_mv_2` order by key;
select key from `masking_test_view_n_mv_2` order by key;

drop materialized view `masking_test_view_n_mv`;
drop materialized view `masking_test_view_n_mv_2`;
drop materialized view `masking_test_view_n_mv_3`;
drop materialized view `masking_test_view_n_mv_4`;
drop table `masking_test_n_mv`;
drop table `srcTnx`;
