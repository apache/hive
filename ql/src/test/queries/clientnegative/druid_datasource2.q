SET metastore.strict.managed.tables=true;
CREATE TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("property" = "localhost", "druid.datasource" = "mydatasource");
