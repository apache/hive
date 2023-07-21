

set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

create table drop_table_auth_fail_1 (key int, value string) partitioned by (ds string);

GRANT All on table drop_table_auth_fail_1 to user hive_test_user;

-- Drop existing regular table WITHOUT DB Drop Privileges

set hive.security.authorization.enabled=true;
DROP TABLE drop_table_auth_fail_1;

