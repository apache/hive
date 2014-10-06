set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_notbucketed(a int, b varchar(128)) partitioned by (ds string) stored as orc TBLPROPERTIES ('transactional'='true');

update acid_notbucketed set b = 'fred' where a = 3;
