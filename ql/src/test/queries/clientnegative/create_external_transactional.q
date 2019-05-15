set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create transactional external table transactional_external (a int, b varchar(128)) clustered by (b) into 2 buckets;