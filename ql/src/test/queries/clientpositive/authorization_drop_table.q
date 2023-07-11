set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

Create database auth_drop_table;

use auth_drop_table;

create table drop_table_auth_1 (key int, value string) partitioned by (ds string);

grant All on table drop_table_auth_1 to user hive_test_user;

GRANT DROP ON DATABASE auth_drop_table TO USER hive_test_user;

show grant user hive_test_user on table drop_table_auth_1;

CREATE TEMPORARY TABLE drop_temp_table LIKE drop_table_auth_1;

set hive.security.authorization.enabled=true;

-- Drop table works fine as user has privs for both DB and table
drop table if exists drop_table_auth_1;

-- Dropping temporary table does not require authorization
drop table if exists drop_temp_table;

