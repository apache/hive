set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set role ALL;
SELECT TRANSFORM (*) USING 'cat' AS (key, value) FROM src;
