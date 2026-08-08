set hive.vectorized.execution.enabled=true;

CREATE TABLE dummy_string_timestamp (
  c1 string,
  c2 string
) STORED AS PARQUET;

INSERT INTO dummy_string_timestamp VALUES 
('2023-01-01 01:02:03', '2023-01-01 01:02:03'),
('invalid_timestamp', '2023-01-02 01:02:03'),
(NULL, '2023-01-03 01:02:03');

CREATE EXTERNAL TABLE test_parquet_timestamp (
  c1 timestamp,
  c2 timestamp
) STORED AS PARQUET
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/dummy_string_timestamp';

CREATE TABLE small_table_timestamp (
  c1 timestamp
);
INSERT INTO small_table_timestamp VALUES ('2023-01-01 01:02:03'), ('2023-01-02 01:02:03');

SET hive.auto.convert.join=true;
SET hive.vectorized.execution.enabled=false;

SELECT /*+ MAPJOIN(small_table_timestamp) */ a.c1
FROM test_parquet_timestamp a
JOIN small_table_timestamp b ON a.c2 = b.c1;

SET hive.vectorized.execution.enabled=true;

EXPLAIN VECTORIZATION DETAIL
SELECT /*+ MAPJOIN(small_table_timestamp) */ a.c1
FROM test_parquet_timestamp a
JOIN small_table_timestamp b ON a.c2 = b.c1;

SELECT /*+ MAPJOIN(small_table_timestamp) */ a.c1
FROM test_parquet_timestamp a
JOIN small_table_timestamp b ON a.c2 = b.c1;
