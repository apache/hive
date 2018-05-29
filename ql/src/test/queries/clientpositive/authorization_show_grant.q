set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set user.name=hive_admin_user;
set role admin;

-- test show grant authorization

create role roleA;
create role roleB;

grant role roleA to user userA;
grant role roleB to role roleA;

set user.name=user1;

-- create table and grant privileges to a role
create table t1_n15(i int, j int, k int);
create table t2_n9(i int, j int, k int);

grant select on t1_n15 to role roleA;
grant insert on t2_n9 to role roleA;
grant insert on t2_n9 to role roleB;

grant insert,delete on t1_n15 to user userA;
grant select,insert on t2_n9 to user userA;


set user.name=hive_admin_user;
set role admin;

-- as user in admin role, it should be possible to see other users grant
show grant user user1 on table t1_n15;
show grant user user1;
show grant role roleA on table t1_n15;
show grant role roleA;
show grant;


set user.name=userA;
-- user belonging to role should be able to see it
show grant role roleA on table t1_n15;
show grant role roleA;

show grant role roleB on table t1_n15;
show grant role roleB;

show grant user userA on table t1_n15;
show grant user userA;


