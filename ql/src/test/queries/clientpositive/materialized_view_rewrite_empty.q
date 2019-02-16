-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.stats.fetch.column.stats=true;
set hive.materializedview.rewriting=true;

create table emps_mv_rewrite_empty (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as orc TBLPROPERTIES ('transactional'='true');

create materialized view emps_mv_rewrite_empty_mv1 as
select * from emps_mv_rewrite_empty where empid < 150;

explain
select * from emps_mv_rewrite_empty where empid < 120;

select * from emps_mv_rewrite_empty where empid < 120;

drop materialized view emps_mv_rewrite_empty_mv1;
drop table emps_mv_rewrite_empty;
