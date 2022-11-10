--! qt:dataset:src

set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
EXPLAIN SELECT * FROM src;
