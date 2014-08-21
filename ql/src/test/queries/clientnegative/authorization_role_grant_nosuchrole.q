set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;

set role ADMIN;

----------------------------------------
-- granting role to a role that does not exist should fail
----------------------------------------

create role role1;
grant role1 to role nosuchrole;
