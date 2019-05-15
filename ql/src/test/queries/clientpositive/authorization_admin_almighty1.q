set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_test_user;

-- actions from admin should work as if admin has all privileges

create table t1_n76(i int);
set user.name=hive_admin_user;

show current roles;
set role ADMIN;
show current roles;
select * from t1_n76;
grant all on table t1_n76 to user user1;
show grant user user1 on table t1_n76;
drop table t1_n76;
