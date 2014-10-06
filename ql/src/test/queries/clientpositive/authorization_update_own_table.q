set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;


set user.name=user1;
create table auth_noupd(i int) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
update auth_noupd set i = 0 where i > 0;

set user.name=hive_admin_user;
set role admin;
