set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE kv_mm(key int, val string) tblproperties ("transactional"="true", "transactional_properties"="insert_only");
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE kv_mm;