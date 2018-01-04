DROP TABLE hbase_table_1;
CREATE TABLE hbase_table_1(key int comment 'It is a column key', value string comment 'It is the column string value')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:string")
TBLPROPERTIES ("hbase.mapreduce.hfileoutputformat.table.name" = "hbase_table_0");

INSERT INTO hbase_table_1 VALUES(1, 'value1');

CREATE EXTERNAL TABLE hbase_table_2(key int comment 'It is a column key', value string comment 'It is the column string value')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:string")
TBLPROPERTIES ("hbase.mapreduce.hfileoutputformat.table.name" = "hbase_table_0");

SELECT * FROM hbase_table_2;
