set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;

CREATE TEMPORARY TABLE auth_temp_table_1(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;

-- Drop temporary table WITHOUT DB Drop Privileges

set hive.security.authorization.enabled=true;
DROP TABLE auth_temp_table_1;