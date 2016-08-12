set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
PARTITIONED BY (dt string)
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");
