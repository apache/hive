set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=hive_admin_user;
set role ADMIN;

create role accounting;

set user.name=user1;
-- user does not belong to this role, so the show role grant should fail
show role grant role accounting;
