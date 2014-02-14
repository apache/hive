set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

create table t1(i int, j int, k int);

-- protecting certain columns
create view vt1 as select i,k from t1;

-- protecting certain rows
create view vt2 as select * from t1 where i > 1;

--view grant to user

grant select on view vt1 to user user2;
grant insert on view vt1 to user user3;

show grant user user2 on table vt1;
show grant user user3 on table vt1;

set user.name=user2;
select * from vt1;

set user.name=user1;

grant all on view vt2 to user user2;
show grant user user2 on table vt2;

revoke all on view vt2 from user user2;
show grant user user2 on table vt2;

revoke select on view vt1 from user user2;
show grant user user2 on table vt1;
show grant user user3 on table vt1;
