set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;

set role ADMIN;

----------------------------------------
-- grant role with admin option, then revoke admin option
-- once the admin option has been revoked, last grant should fail
----------------------------------------

create role src_role_wadmin;
grant  src_role_wadmin to user user2 with admin option;
show role grant user user2;


set user.name=user2;
set role src_role_wadmin;
grant  src_role_wadmin to user user3;
revoke src_role_wadmin from user user3;

set user.name=hive_admin_user;
set role ADMIN;
revoke admin option for src_role_wadmin from user user2;
show role grant user user2;
set user.name=user2;
set role src_role_wadmin;
-- grant/revoke should now fail
grant  src_role_wadmin to user user3;
