set hive.vectorized.execution.enabled=false;
set parquet.column.index.access=true;

-- Test parquet table with Timestamp Col in int64 format to BigInt conversion
-- This parquet file in the data/files directory was created via the steps
-- documented in HIVE-26612

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/parquet_format_ts_as_bigint;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/parquet_format_ts_as_bigint/part-00000;
dfs -copyFromLocal ../../data/files/timestamp_as_bigint.parquet ${system:test.tmp.dir}/parquet_format_ts_as_bigint/part-00000;

DROP TABLE ts_bigint_pq;

-- Use data from parquet file that uses TS as a BIGINT

CREATE EXTERNAL TABLE ts_as_bigint_pq (dummy int, ts2 BIGINT)
    STORED AS PARQUET
    LOCATION '${system:test.tmp.dir}/parquet_format_ts_as_bigint';

SELECT * FROM ts_as_bigint_pq;

dfs -rmr ${system:test.tmp.dir}/parquet_format_ts_as_bigint;
