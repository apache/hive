set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

-- run a sql query to initialize authorization, then try setting a non-existent config param
use default;
set hive.exec.reduce.max=1;
