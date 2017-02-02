set hive.strict.checks.cartesian.product=false;
set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler2'
TBLPROPERTIES ("druid.datasource" = "wikipedia");
