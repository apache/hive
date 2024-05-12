--! qt:timezone:Asia/Singapore

-- Test 1
-- insert into default.test_sgt700 values ('0200-03-01 00:00:00')
CREATE EXTERNAL TABLE `TEST_SGT`(`currtime` timestamp) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

LOAD DATA LOCAL INPATH '../../data/files/sgt000' INTO TABLE TEST_SGT;
SELECT * FROM TEST_SGT;

-- Test 2
-- Inserted data in singapore timezone: insert into default.test_sgt700 values ('0500-03-01 00:00:00'),('0600-03-01
-- 00:00:00'),('0700-03-01 00:00:00'), ('0200-03-01 00:00:00'), ('0400-03-01 00:00:00'), ('0800-03-02 00:00:00'),
--('0800-03-03 00:00:00');

CREATE EXTERNAL TABLE `TEST_SGT1`(`currtime` timestamp) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

LOAD DATA LOCAL INPATH '../../data/files/800' INTO TABLE TEST_SGT1;
SELECT * FROM TEST_SGT1;
