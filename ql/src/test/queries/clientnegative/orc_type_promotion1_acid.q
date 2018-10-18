set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

SET hive.exec.schema.evolution=false;

-- Currently, string to int conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key string, val string) clustered by (val) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
alter table src_orc change key key int;
