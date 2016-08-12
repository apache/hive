set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
LOCATION '/testfolder/'
TBLPROPERTIES ("druid.datasource" = "wikipedia");
