set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

create table amvdo_table (a int, b varchar(256), c decimal(10,2));

insert into amvdo_table values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view amvdo_mat_view as select a, c from amvdo_table;

set user.name=user2;
drop materialized view amvdo_mat_view;
