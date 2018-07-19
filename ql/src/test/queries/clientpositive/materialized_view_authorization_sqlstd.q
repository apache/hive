set hive.vectorized.execution.enabled=false;
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

create table amvs_table (a int, b varchar(256), c decimal(10,2));

insert into amvs_table values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view amvs_mat_view disable rewrite as select a, c from amvs_table;

show grant user user1 on table amvs_mat_view;

grant select on amvs_mat_view to user user2;

set user.name=user2;
show grant user user2 on table amvs_mat_view;
select * from amvs_mat_view;

set user.name=user3;
show grant user user3 on table amvs_mat_view;


set user.name=hive_admin_user;
set role admin;
show grant on table amvs_mat_view;

set user.name=user1;
revoke select on table amvs_mat_view from user user2;
set user.name=user2;
show grant user user2 on table amvs_mat_view;

set user.name=hive_admin_user;
set role ADMIN;
create role role_v;
grant  role_v to user user4 ;
show role grant user user4;
show roles;

grant all on table amvs_mat_view to role role_v;
show grant role role_v on table amvs_mat_view;
show grant user user4 on table amvs_mat_view;
select * from amvs_mat_view;

set user.name=user1;
grant select on table amvs_table to user user2 with grant option;
set user.name=user2;
create materialized view amvs_mat_view2 disable rewrite as select a, b from amvs_table;

select * from amvs_mat_view2;

drop materialized view amvs_mat_view2;

set user.name=hive_admin_user;
set role ADMIN;
