set hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE table_acid(d int, tz timestamp with local time zone)
    clustered by (d) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');