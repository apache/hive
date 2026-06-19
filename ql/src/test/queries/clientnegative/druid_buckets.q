set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
CLUSTERED BY (robot) INTO 32 BUCKETS
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");
