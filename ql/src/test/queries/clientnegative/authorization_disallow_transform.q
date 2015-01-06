set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authorization.enabled=true;
set hive.entity.capture.transform=true;
set role ALL;
create table t1(i int);
SELECT TRANSFORM (*) USING 'cat' AS (key, value) FROM t1;
