set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
add jar ${system:maven.local.repository}/org/apache/hive/hcatalog/hive-hcatalog-core/${system:hive.version}/hive-hcatalog-core-${system:hive.version}.jar;

CREATE TABLE parquet_table_json_partition (
id bigint COMMENT 'from deserializer',
address struct<country:bigint,state:bigint> COMMENT 'from deserializer',
reports array<bigint> COMMENT 'from deserializer')
PARTITIONED BY (
ts string)
ROW FORMAT SERDE
'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

LOAD DATA LOCAL INPATH '../../data/files/sample2.json' INTO TABLE parquet_table_json_partition PARTITION(ts='20150101');

SELECT * FROM parquet_table_json_partition ORDER BY address, reports LIMIT 100;

ALTER TABLE parquet_table_json_partition
  SET FILEFORMAT INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                 OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                 SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe';

SELECT * FROM parquet_table_json_partition ORDER BY address, reports LIMIT 100;

CREATE TABLE new_table AS SELECT * FROM parquet_table_json_partition ORDER BY address, reports LIMIT 100;

SELECT * FROM new_table ORDER BY address, reports;


