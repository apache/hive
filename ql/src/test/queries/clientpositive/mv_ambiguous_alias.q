set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.query.lifetime.hooks=;

create table t1 (key int, value int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t1 values (1,1), (2,1), (3,3);

create materialized view mv1 as
select * from t1 where key < 6;

set hive.cbo.rule.exclusion.regex=HiveJoinAddNotNullRule;
explain cbo
select * from 
  (select * from t1 a where key < 6) subq1 
    join
  (select * from t1 a where key < 6) subq2
  on (subq1.key = subq2.key)
    join
  (select * from t1 a where key < 6) subq3
  on (subq1.key = subq3.key);