set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

CREATE DATABASE auth_db_fail_2;
use auth_db_fail_2;
create table drop_table_auth_4 (key int, value string) partitioned by (ds string);

-- Drop existing regular table with IF EXISTS from current database

set hive.security.authorization.enabled=true;
DROP TABLE IF EXISTS drop_table_auth_4;
