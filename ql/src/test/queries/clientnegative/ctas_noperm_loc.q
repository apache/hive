set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} hdfs:///tmp/ctas_noperm_loc;

set user.name=user1;

create table foo0 location 'hdfs:///tmp/ctas_noperm_loc_foo0' as select 1 as c1;
create table foo1 location 'hdfs:///tmp/ctas_noperm_loc/foo1' as select 1 as c1;
