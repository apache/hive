set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;

-- run a sql query to initialize authorization, then try setting a allowed config and then a disallowed config param
use default;
set hive.optimize.listbucketing=true;
set hive.security.authorization.enabled=true;
