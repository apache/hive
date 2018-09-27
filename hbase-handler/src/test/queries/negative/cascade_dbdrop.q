
CREATE DATABASE hbaseDB;

-- Hadoop 0.23 changes the behavior FsShell on Exit Codes
-- In Hadoop 0.20
-- Exit Code == 0 on success
-- Exit code < 0 on any failure
-- In Hadoop 0.23
-- Exit Code == 0 on success
-- Exit Code < 0 on syntax/usage error
-- Exit Code > 0 operation failed

CREATE EXTERNAL TABLE hbaseDB.hbase_table_0(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string")
TBLPROPERTIES ("hbase.table.name" = "hbase_table_0", "external.table.purge" = "true");

dfs -ls ${hiveconf:hbase.rootdir}/data/default/hbase_table_0;

DROP DATABASE IF EXISTS hbaseDB CASCADE;

dfs -ls ${hiveconf:hbase.rootdir}/data/default/hbase_table_0;
