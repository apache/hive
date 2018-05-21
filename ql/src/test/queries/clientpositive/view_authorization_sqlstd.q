set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

-- Test view authorization , and 'show grant' variants

create table t1_n54(i int, j int, k int);
grant select on t1_n54 to user user2 with grant option;
show grant user user1 on table t1_n54;

-- protecting certain columns
create view vt1_n54 as select i,k from t1_n54;

-- protecting certain rows
create view vt2 as select * from t1_n54 where i > 1;

show grant user user1 on all;

--view grant to user
-- try with and without table keyword

grant select on vt1_n54 to user user2;
grant insert on table vt1_n54 to user user3;

set user.name=user2;
show grant user user2 on table vt1_n54;
create view vt3 as select i,k from t1_n54;

set user.name=user3;
show grant user user3 on table vt1_n54;


set user.name=user2;

explain authorization select * from vt1_n54;
select * from vt1_n54;

-- verify input objects required does not include table
-- even if view is within a sub query
select * from (select * from vt1_n54) a;

select * from vt1_n54 union all select * from vt1_n54;

set user.name=user1;

grant all on table vt2 to user user2;

set user.name=user2;
show grant user user2 on table vt2;
show grant user user2 on all;
set user.name=user1;

revoke all on vt2 from user user2;

set user.name=user2;
show grant user user2 on table vt2;


set user.name=hive_admin_user;
set role admin;
show grant on table vt2;

set user.name=user1;
revoke select on table vt1_n54 from user user2;

set user.name=user2;
show grant user user2 on table vt1_n54;
show grant user user2 on all;

set user.name=user3;
-- grant privileges on roles for view, after next statement
show grant user user3 on table vt1_n54;

set user.name=hive_admin_user;
show current roles;
set role ADMIN;
create role role_v;
grant  role_v to user user4 ;
show role grant user user4;
show roles;

grant all on table vt2 to role role_v;
show grant role role_v on table vt2;

revoke delete on table vt2 from role role_v;
show grant role role_v on table vt2;
show grant on table vt2;
