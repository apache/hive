set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table t1 (key int, value int) stored as orc TBLPROPERTIES ('transactional'='true');

create materialized view mv as
select * from t1 where key < 6;

explain cbo
select * from 
  (select * from t1 where key < 6) a join
  (select * from t1 where key < 6) b join
  (select * from t1 where key < 6) c;
