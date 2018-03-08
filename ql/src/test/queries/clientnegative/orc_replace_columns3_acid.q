set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

SET hive.exec.schema.evolution=false;

-- Currently, smallint to tinyint conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key smallint, val string) clustered by (val) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
alter table src_orc replace columns (k int, val string, z smallint);
alter table src_orc replace columns (k int, val string, z tinyint);
