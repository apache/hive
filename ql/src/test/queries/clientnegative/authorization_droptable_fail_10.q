set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

CREATE DATABASE auth_db_fail_4;
use auth_db_fail_4;
create temporary table drop_table_temp_4 (key int, value string) partitioned by (ds string);

-- Drop temporary table from current database with IF EXISTS

set hive.security.authorization.enabled=true;
DROP TABLE IF EXISTS drop_table_temp_4;