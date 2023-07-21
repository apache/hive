set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

CREATE DATABASE auth_db_fail_2;
use auth_db_fail_2;
create table drop_table_auth_4 (key int, value string) partitioned by (ds string);
GRANT All on table auth_db_fail_2.drop_table_auth_4 to user hive_test_user;

-- Drop existing regular table from current database

set hive.security.authorization.enabled=true;
DROP TABLE IF EXISTS drop_table_auth_4;