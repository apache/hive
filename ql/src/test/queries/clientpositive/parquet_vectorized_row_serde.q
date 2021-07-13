set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table tbl_parquet;

CREATE TABLE `tbl_parquet`(
   `sample_sk` int
)
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';

INSERT INTO tbl_parquet VALUES (1);

-- Vectorized
-- Vectorized Reader should be ignoring row.serde
set hive.vectorized.use.vectorized.input.format=true;
set hive.vectorized.use.row.serde.deserialize=false;

explain vectorization detail
select SUM(sample_sk) FROM tbl_parquet;
select SUM(sample_sk) FROM tbl_parquet;

-- Vectorized
set hive.vectorized.use.row.serde.deserialize=true;
explain vectorization detail
select SUM(sample_sk) FROM tbl_parquet;
select SUM(sample_sk) FROM tbl_parquet;

-- NOT Vectorized
set hive.vectorized.use.vectorized.input.format=false;
set hive.vectorized.use.row.serde.deserialize=false;
explain vectorization detail
select SUM(sample_sk) FROM tbl_parquet;
select SUM(sample_sk) FROM tbl_parquet;

-- NOT Vectorized
-- Row.serde on a Vectorized reader should return:
--    Row deserialization of vectorized input format not supported
set hive.vectorized.use.row.serde.deserialize=true;
explain vectorization detail
select SUM(sample_sk) FROM tbl_parquet;
select SUM(sample_sk) FROM tbl_parquet;