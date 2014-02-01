set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
-- this is applicable to any security mode as check is in metastore
create role role1;
create role role2;
grant role role1 to role role2;

-- this will create a cycle
grant role role2 to role role1;