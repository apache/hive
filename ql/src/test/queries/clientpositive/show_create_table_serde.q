-- Test SHOW CREATE TABLE on a table with serde.

CREATE TABLE tmp_showcrt1_n0 (key int, value string, newvalue bigint);
ALTER TABLE tmp_showcrt1_n0 SET SERDEPROPERTIES ('custom.property.key1'='custom.property.value1', 'custom.property.key2'='custom.property.value2');
SHOW CREATE TABLE tmp_showcrt1_n0;
DROP TABLE tmp_showcrt1_n0;

-- without a storage handler
CREATE TABLE tmp_showcrt1_n0 (key int, value string, newvalue bigint)
COMMENT 'temporary table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';
SHOW CREATE TABLE tmp_showcrt1_n0;
DROP TABLE tmp_showcrt1_n0;

-- without a storage handler / with custom serde params
CREATE TABLE tmp_showcrt1_n0 (key int, value string, newvalue bigint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
WITH SERDEPROPERTIES ('custom.property.key1'='custom.property.value1', 'custom.property.key2'='custom.property.value2')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';
SHOW CREATE TABLE tmp_showcrt1_n0;
DROP TABLE tmp_showcrt1_n0;

-- with a storage handler and serde properties
CREATE EXTERNAL TABLE tmp_showcrt1_n0 (key string, value boolean)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
WITH SERDEPROPERTIES ('field.delim'=',', 'serialization.format'='$');
SHOW CREATE TABLE tmp_showcrt1_n0;
DROP TABLE tmp_showcrt1_n0;

