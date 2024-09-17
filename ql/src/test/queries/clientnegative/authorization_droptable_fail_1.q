set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- Drop non-existing table WITHOUT DB Drop Privileges

CREATE DATABASE auth_db_fail;

set hive.security.authorization.enabled=true;

DROP TABLE auth_db_fail.auth_permanent_table;
