--! qt:dataset:src
CREATE EXTERNAL TABLE hbase_pushdown(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:string",
"hbase.scan.cache" = "500", "hbase.scan.cacheblocks" = "true", "hbase.scan.batch" = "1")
TBLPROPERTIES ("external.table.purge" = "true");

INSERT OVERWRITE TABLE hbase_pushdown SELECT * FROM src;

select * from hbase_pushdown;
