set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;

create table src_autho_test (key STRING, value STRING) ;

set hive.security.authorization.enabled=true;

--select dummy table
select 1;

set  role ADMIN; 
--table grant to user

grant select on table src_autho_test to user user_sauth;

show grant user user_sauth on table src_autho_test;


revoke select on table src_autho_test from user user_sauth;
show grant user user_sauth on table src_autho_test;

--role
create role src_role;
grant role src_role to user user_sauth;
show role grant user user_sauth;

--table grant to role

-- also verify case insesitive behavior of role name
grant select on table src_autho_test to role Src_ROle;

show grant role src_role on table src_autho_test;
revoke select on table src_autho_test from role src_rolE;

-- drop role
drop role SRc_role;

set hive.security.authorization.enabled=false;
drop table src_autho_test;
