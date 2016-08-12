set hive.druid.broker.address.default=;

CREATE EXTERNAL TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");
