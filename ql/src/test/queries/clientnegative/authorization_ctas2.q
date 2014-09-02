set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set user.name=user_dbowner;
-- check ctas without db ownership
create database ctas_auth;

set user.name=user_unauth;
create table t1(i int);
use ctas_auth;
show tables;
create table t2 as select * from default.t1;
