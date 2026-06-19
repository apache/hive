set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_notbucketed(a int, b varchar(128)) TBLPROPERTIES ('transactional'='true');

