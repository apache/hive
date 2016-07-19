set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

-- check insert without select priv
create table testp(i int) partitioned by (dt string);
grant select on table testp to user user1;

set user.name=user1;
create table user2tab(i int);
explain authorization insert into table testp partition (dt = '2012')  values (1);
explain authorization insert overwrite table testp partition (dt = '2012')  values (1);
insert into table testp partition (dt = '2012')  values (1);
insert overwrite table testp partition (dt = '2012')  values (1);
