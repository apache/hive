set hive.druid.broker.address.default=localhost.test;

-- no timestamp
EXPLAIN
CREATE TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia")
AS
SELECT key, value FROM src;
