--! qt:timezone:Asia/Singapore

CREATE EXTERNAL TABLE `TEST_SGT`(`currtime` timestamp) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

LOAD DATA LOCAL INPATH '../../data/files/sgt000' INTO TABLE TEST_SGT;
SELECT * FROM TEST_SGT;
