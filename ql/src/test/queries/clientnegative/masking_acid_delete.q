set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table masking_test (key int, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");

delete from masking_test where value='ddd';
