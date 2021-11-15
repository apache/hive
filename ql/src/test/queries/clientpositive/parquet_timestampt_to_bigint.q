set hive.vectorized.execution.enabled=false;
set parquet.column.index.access=true;

-- Test paquet table with Timestamp Col to BigInt convertion
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/parquet_format_ts;

DROP TABLE ts_pq;

CREATE EXTERNAL TABLE ts_pq (ts1 TIMESTAMP)
    STORED AS PARQUET
    LOCATION '${system:test.tmp.dir}/parquet_format_ts';

INSERT INTO ts_pq VALUES ('1998-10-03 09:58:31.231');

SELECT * FROM ts_pq;

-- Now use data from another table that uses TS as a BIGINT

CREATE EXTERNAL TABLE ts_pq_2 (ts2 BIGINT)
    STORED AS PARQUET
    LOCATION '${system:test.tmp.dir}/parquet_format_ts';

SELECT * FROM ts_pq_2;

dfs -rmr ${system:test.tmp.dir}/parquet_format_ts;