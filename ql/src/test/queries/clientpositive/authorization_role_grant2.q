set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set hive.cli.print.header=true;
set user.name=hive_admin_user;
set role ADMIN;

----------------------------------------
-- role granting with admin option
----------------------------------------
-- Also test case sensitivity of role name

create role srC_role_wadmin;
create role src_roLe2;
grant src_role_wadmin to user user2 with admin option;
show role grant user user2;
show principals src_role_wadmin;


set user.name=user2;
set role src_role_WadMin;
show principals src_role_wadmin;
-- grant role to another user
grant src_Role_wadmin to user user3;

set user.name=user3;
show role grant user user3;

set user.name=user2;
-- grant role to another role
grant src_role_wadmin to role sRc_role2;

set user.name=hive_admin_user;
set role ADMIn;
grant src_role2 to user user3;

set user.name=user3;
-- as user3 belings to src_role2 hierarchy, its should be able to run show grant on it
show role grant role src_Role2;

set user.name=hive_admin_user;
set role ADMIN;



show principals src_ROle_wadmin;

set user.name=user2;
set role src_role_wadmin;
-- revoke user from role
revoke src_rolE_wadmin from user user3;

set user.name=user3;
show role grant user user3;
set user.name=user2;

-- revoke role from role
revoke src_rolE_wadmin from role sRc_role2;
set user.name=hive_admin_user;
set role ADMIN;

show role grant role sRc_role2;

show principals src_role_wadmin;
