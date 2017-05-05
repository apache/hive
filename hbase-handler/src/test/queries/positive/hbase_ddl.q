DROP TABLE hbase_table_1;
CREATE TABLE hbase_table_1(key int comment 'It is a column key', value string comment 'It is the column string value')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:string")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_0");

DESCRIBE EXTENDED hbase_table_1;

select * from hbase_table_1;

EXPLAIN FROM src INSERT OVERWRITE TABLE hbase_table_1 SELECT * WHERE (key%2)=0;
FROM src INSERT OVERWRITE TABLE hbase_table_1 SELECT * WHERE (key%2)=0;

ALTER TABLE hbase_table_1 SET TBLPROPERTIES('hbase.mapred.output.outputtable'='kkk');

desc formatted hbase_table_1;

ALTER TABLE hbase_table_1 unset TBLPROPERTIES('hbase.mapred.output.outputtable');

desc formatted hbase_table_1;
