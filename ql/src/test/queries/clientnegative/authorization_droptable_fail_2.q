set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

CREATE DATABASE auth_db_fail_1;

set hive.security.authorization.enabled=true;

-- Drop non-existing table with IF EXISTS clause WITHOUT DB Drop Privileges

DROP TABLE IF EXISTS auth_db_fail_1.auth_permanent_table;
