-- -*- mode:sql -*-

DROP TABLE IF EXISTS hbase_bulk;

CREATE TABLE hbase_bulk (key INT, value STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:string');

SET hive.hbase.generatehfiles = true;
INSERT OVERWRITE TABLE hbase_bulk SELECT * FROM src CLUSTER BY key;
