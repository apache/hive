set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table drop_table_auth_fail_1 (key int, value string) partitioned by (ds string);

-- Drop existing table WITHOUT DB Drop Privileges

set hive.security.authorization.enabled=true;

DROP TABLE drop_table_auth_fail_1;
