set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} hdfs:///tmp/ct_noperm_loc;

set user.name=user1;

create table foo0(id int) location 'hdfs:///tmp/ct_noperm_loc_foo0';
create table foo1(id int) location 'hdfs:///tmp/ct_noperm_loc/foo1';
