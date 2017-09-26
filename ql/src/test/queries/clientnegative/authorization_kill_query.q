set hive.security.authorization.enabled=true;
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=hive_admin_user;
set role ADMIN;
explain authorization kill query 'dummyqueryid';
kill query 'dummyqueryid';

set user.name=ruser1;

-- kill query as non-admin should fail
explain authorization kill query 'dummyqueryid';
kill query 'dummyqueryid';
