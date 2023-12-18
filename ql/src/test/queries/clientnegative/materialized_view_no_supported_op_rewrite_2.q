set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

CREATE TABLE EMPS (ENAME STRING, BIRTH_EPOCH_SECS INT) STORED AS ORC TBLPROPERTIES ('transactional'='true');

create materialized view cmv_mat_view disable rewrite as SELECT * FROM EMPS WHERE BIRTH_EPOCH_SECS <= UNIX_TIMESTAMP();

alter materialized view cmv_mat_view enable rewrite;
