set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

CREATE DATABASE auth_db_fail_1;
use auth_db_fail_1;
create table drop_table_auth_3 (key int, value string) partitioned by (ds string);

-- Drop existing regular table from current database

set hive.security.authorization.enabled=true;
DROP TABLE drop_table_auth_3;
