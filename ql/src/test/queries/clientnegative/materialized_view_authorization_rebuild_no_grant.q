set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

create table amvrng_table (a int, b varchar(256), c decimal(10,2));

insert into amvrng_table values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

grant select on table amvrng_table to user user2 with grant option;

set user.name=user2;
create materialized view amvrng_mat_view as select a, c from amvrng_table;

set user.name=user1;
alter materialized view amvrng_mat_view rebuild;
