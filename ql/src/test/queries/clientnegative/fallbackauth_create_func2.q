set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;

-- temp function creation should fail for non-admin roles
create temporary function temp_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

