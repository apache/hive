set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

dfs -ls dummy_file;
