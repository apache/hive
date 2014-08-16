set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=ruser1;
show role grant user ruser1;

set user.name=hive_admin_user;
set role ADMIN;
show role grant user ruser1;
show role grant user ruser2;

set user.name=ruser1;
--  show role grant for another user as non admin user should fail
show role grant user ruser2;
