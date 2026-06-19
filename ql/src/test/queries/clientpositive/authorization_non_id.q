set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;

set role ADMIN;
drop table if exists src_autho_test_n12;
create table src_autho_test_n12 (id int);

create role src_role2;

grant role src_role2 to user bar;
grant role src_role2 to user `foo-1`;

show role grant user bar;
show role grant user `foo-1`;

grant select on table src_autho_test_n12 to user bar;
grant select on table src_autho_test_n12 to user `foo-1`;

show grant user bar on all;
show grant user `foo-1` on all;

drop table src_autho_test_n12;
drop role src_role2;
