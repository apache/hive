--! qt:dataset:src
-- -*- mode:sql -*-

DROP TABLE IF EXISTS hbase_bulk;

CREATE EXTERNAL TABLE hbase_bulk (key INT, value STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:string')
TBLPROPERTIES ("external.table.purge" = "true");

SET hive.hbase.generatehfiles = true;
INSERT OVERWRITE TABLE hbase_bulk SELECT * FROM src CLUSTER BY key;
