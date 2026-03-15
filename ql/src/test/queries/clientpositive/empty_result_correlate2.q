set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- To create a HiveValues operator before decorrelation automatic query rewrite with
-- materialized views is used.
-- All tables must be transactional to trigger automatic query rewrite.
create table mv_source (any_col int) stored as orc TBLPROPERTIES ('transactional'='true');
create table t1 (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true');
create table t2 (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true');

-- At least one materialized view must exist.
create materialized view any_mv as
select any_col from mv_source where any_col > 20;

-- Automatic query rewrite traverses the plan and recreates the HiveFilter using RelBuilder.
-- If the condition is always false, it replaces the filter with HiveValues.
explain cbo
select * from t1 where 1 = 2
union
select * from t1
where exists (select a from t2 where t1.a > 10)
;
