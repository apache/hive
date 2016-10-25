set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

-- check truncate on partition
create table auth_trunc2(i int) partitioned by (j int);
alter table auth_trunc2 add partition (j=42);
set user.name=user1;
truncate table auth_trunc2 partition (j=42);

