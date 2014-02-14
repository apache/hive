set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set role NONE;
SELECT TRANSFORM (*) USING 'cat' AS (key, value) FROM src;
