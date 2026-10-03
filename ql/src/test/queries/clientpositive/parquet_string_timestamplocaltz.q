set hive.vectorized.execution.enabled=true;

CREATE TABLE dummy_string_timestamplocaltz (
  c1 string,
  c2 string
) STORED AS PARQUET;

INSERT INTO dummy_string_timestamplocaltz VALUES 
('2023-01-01 01:02:03 America/Los_Angeles', '2023-01-01 01:02:03 America/Los_Angeles'),
('invalid_timestamp_tz', '2023-01-02 01:02:03 America/Los_Angeles'),
(NULL, '2023-01-03 01:02:03 America/Los_Angeles');

CREATE EXTERNAL TABLE test_parquet_timestamplocaltz (
  c1 timestamp with local time zone,
  c2 timestamp with local time zone
) STORED AS PARQUET
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/dummy_string_timestamplocaltz';

CREATE TABLE small_table_timestamplocaltz (
  c1 timestamp with local time zone
);
INSERT INTO small_table_timestamplocaltz VALUES ('2023-01-01 01:02:03 America/Los_Angeles'), ('2023-01-02 01:02:03 America/Los_Angeles');

SET hive.auto.convert.join=true;
SET hive.vectorized.execution.enabled=false;

SELECT /*+ MAPJOIN(small_table_timestamplocaltz) */ a.c1
FROM test_parquet_timestamplocaltz a
JOIN small_table_timestamplocaltz b ON a.c2 = b.c1;

SET hive.vectorized.execution.enabled=true;

EXPLAIN VECTORIZATION DETAIL
SELECT /*+ MAPJOIN(small_table_timestamplocaltz) */ a.c1
FROM test_parquet_timestamplocaltz a
JOIN small_table_timestamplocaltz b ON a.c2 = b.c1;

SELECT /*+ MAPJOIN(small_table_timestamplocaltz) */ a.c1
FROM test_parquet_timestamplocaltz a
JOIN small_table_timestamplocaltz b ON a.c2 = b.c1;
