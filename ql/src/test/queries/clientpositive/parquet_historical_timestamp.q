--! qt:timezone:Asia/Singapore
--These files were created by inserting timestamp '2019-01-01 00:30:30.111111111' where writer time zone is Europe/Rome.

--older writer: time zone dependent behavior. convert to reader time zone
create table legacy_table (t timestamp) stored as parquet;

load data local inpath '../../data/files/parquet_historical_timestamp_legacy.parq' into table legacy_table;

select * from legacy_table;


--newer writer: time zone agnostic behavior. convert to writer time zone
create table new_table (t timestamp) stored as parquet;

load data local inpath '../../data/files/parquet_historical_timestamp_new.parq' into table new_table;

select * from new_table;

-- Inserted data in singapore timezone: insert into default.test_sgt700 values ('0500-03-01 00:00:00'),('0600-03-01
-- 00:00:00'),('0700-03-01 00:00:00'), ('0200-03-01 00:00:00'), ('0400-03-01 00:00:00'), ('0800-03-02 00:00:00'),
--('0800-03-03 00:00:00');

CREATE EXTERNAL TABLE `TEST_SGT1`(`currtime` timestamp) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

LOAD DATA LOCAL INPATH '../../data/files/legacyLeapYearInSingaporeTZ' INTO TABLE TEST_SGT1;
SELECT * FROM TEST_SGT1;
