set hive.exec.drop.ignorenonexistent=false;

set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- Drop non-existing table

CREATE DATABASE auth_db_fail;
GRANT DROP ON DATABASE auth_db_fail TO USER hive_test_user;

set hive.security.authorization.enabled=true;

DROP TABLE auth_db_fail.auth_permanent_table;
