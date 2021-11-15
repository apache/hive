set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE constant_prop(start_dt timestamp, stop_dt timestamp) STORED AS ORC TBLPROPERTIES ('transactional'='true');

set hive.test.currenttimestamp=2020-03-05 14:16:57;
set hive.cbo.enable=false;

explain UPDATE constant_prop SET stop_dt = CURRENT_TIMESTAMP WHERE CAST(start_dt AS DATE) = CURRENT_DATE;
