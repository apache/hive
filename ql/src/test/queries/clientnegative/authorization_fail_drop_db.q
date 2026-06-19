set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=false;
create database db_fail_to_drop;
set hive.security.authorization.enabled=true;

drop database db_fail_to_drop;
