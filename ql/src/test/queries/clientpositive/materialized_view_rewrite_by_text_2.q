-- SORT_QUERY_RESULTS

SET hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=false;

create table cmv_basetable_n0 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

create materialized view if not exists cmv_mat_view2
as select a, c from cmv_basetable_n0 where a = 3;

explain cbo
select a, c from cmv_basetable_n0 where a = 3;

alter materialized view cmv_mat_view2 disable rewrite;

explain cbo
select a, c from cmv_basetable_n0 where a = 3;

alter materialized view cmv_mat_view2 enable rewrite;

explain cbo
select a, c from cmv_basetable_n0 where a = 3;

drop materialized view cmv_mat_view2;

explain cbo
select a, c from cmv_basetable_n0 where a = 3;
