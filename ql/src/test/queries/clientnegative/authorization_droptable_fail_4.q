set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table drop_table_auth_fail_2 (key int, value string);

-- Drop existing regular table with IF EXISTS WITHOUT DB Drop Privileges

set hive.security.authorization.enabled=true;

DROP TABLE IF EXISTS drop_table_auth_fail_2;
