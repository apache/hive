set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE t_auth_del(i int) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- grant update privilege to another user
GRANT DELETE ON t_auth_del TO USER userWIns;
GRANT SELECT ON t_auth_del TO USER userWIns;

set user.name=hive_admin_user;
set role admin;
SHOW GRANT ON TABLE t_auth_del;


set user.name=userWIns;
delete from t_auth_del where i > 0;
