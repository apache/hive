set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;

-- enable sql standard authorization
-- role granting without role keyword
-- also test role being treated as case insensitive
set role ADMIN;
create role src_Role2;

grant SRC_role2 to user user2 ;
show role grant user user2;
show roles;

-- revoke role without role keyword
revoke src_rolE2 from user user2;
show role grant user user2;
show roles;

----------------------------------------
-- role granting without role keyword, with admin option (syntax check)
----------------------------------------

create role src_role_wadmin;
grant src_role_wadmin to user user2 with admin option;
show role grant user user2;

-- revoke admin option
revoke admin option for src_role_wadmin from user user2;
show role grant user user2;

-- revoke role without role keyword
revoke src_role_wadmin from user user2;
show role grant user user2;

-- drop roles
show roles;
drop role Src_role2;
show roles;
drop role sRc_role_wadmin;
show roles;
