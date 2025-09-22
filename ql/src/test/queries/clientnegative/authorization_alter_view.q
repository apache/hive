--! qt:authorizer
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

-- create db1, tab1, view1 as hive_admin_user
set user.name=hive_admin_user;
set role ADMIN;

create database db1;
create table db1.tab1(i int);
create view db1.view1 as select * from db1.tab1;

-- grant select privileges on db1 and view1
GRANT select ON DATABASE db1 TO USER user2;
GRANT select ON TABLE db1.view1 to USER user2;

-- create db2, tab2 as user2
set user.name=user2;
create database db2;
create table db2.tab2(i int);

-- try to alter view1 as user2 and it should fail as user2 doesn't have required privilege
alter view db1.view1 as select * from db2.tab2