--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create table `masking_test` as select cast(key as int) as key, value from src;

analyze table `masking_test` compute statistics for columns;
