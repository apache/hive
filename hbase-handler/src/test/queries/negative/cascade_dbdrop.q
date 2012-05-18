
CREATE DATABASE hbaseDB;

CREATE TABLE hbaseDB.hbase_table_0(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_0");

dfs -ls ../build/ql/tmp/hbase/hbase_table_0;

DROP DATABASE IF EXISTS hbaseDB CASCADE;

dfs -ls ../build/ql/tmp/hbase/hbase_table_0;






