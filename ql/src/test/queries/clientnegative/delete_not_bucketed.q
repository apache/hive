set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_notbucketed(a int, b varchar(128)) stored as orc TBLPROPERTIES ('transactional'='true');

delete from acid_notbucketed where a = 3;
