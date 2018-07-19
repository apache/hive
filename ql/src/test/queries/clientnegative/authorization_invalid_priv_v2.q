set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table if not exists authorization_invalid_v2 (key int, value string);
grant lock on table authorization_invalid_v2 to user hive_test_user;
drop table authorization_invalid_v2;
