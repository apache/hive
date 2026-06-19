set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;

create table t1 (a int, b int) stored as orc TBLPROPERTIES ('transactional'='true');

with cte as (
  select a, count(*) as c1 from t1
  group by a)
select cte1.a, cte1.c1
 from cte cte1,
      cte cte2
where cte1.a = cte2.a
  and cte1.a > 10;
