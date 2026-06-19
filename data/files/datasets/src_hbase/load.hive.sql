DROP table if exists src_hbase_tmp;

CREATE TABLE src_hbase_tmp (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" INTO TABLE src_hbase_tmp;

ANALYZE TABLE src_hbase_tmp COMPUTE STATISTICS;

ANALYZE TABLE src_hbase_tmp COMPUTE STATISTICS FOR COLUMNS key,value;

DROP table if exists src_hbase;

CREATE TABLE src_hbase (key INT, value STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,cf:val')
TBLPROPERTIES ('hbase.table.name' = 'src_hbase');

INSERT OVERWRITE TABLE src_hbase SELECT * FROM src_hbase_tmp
