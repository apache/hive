DROP TABLE hbase_table_1;
CREATE TABLE hbase_table_1(key int comment 'It is a column key', value string comment 'It is the column string value')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:string")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_0");

DESCRIBE EXTENDED hbase_table_1;

alter table hbase_table_1 change column key newkey string;
