set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

create table src_autho_test (key STRING, value STRING) ;

set hive.security.authorization.enabled=true;

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

grant select on table src_autho_test to role src_role;

show grant role src_role on table src_autho_test;
revoke select on table src_autho_test from role src_role;

-- drop role
drop role src_role;

set hive.security.authorization.enabled=false;
drop table src_autho_test;