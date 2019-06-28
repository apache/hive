set hive.vectorized.execution.enabled=false;

create table timetest_parquet(t timestamp) stored as parquet;

load data local inpath '../../data/files/parquet_external_time.parq' into table timetest_parquet;

select * from timetest_parquet;