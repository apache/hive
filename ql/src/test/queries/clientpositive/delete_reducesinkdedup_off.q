set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.optimize.reducededuplication=false;

create table t1(a int) stored as orc TBLPROPERTIES ('transactional'='true');

explain
delete from t1 where a = 3;
