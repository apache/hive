FIXME : crap?
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

create table t1(i int);


create scheduled query s1 cron '* * * * * ? *'
	defined as select * from t1;

set user.name=user1;
create table t2 as select * from t1;

