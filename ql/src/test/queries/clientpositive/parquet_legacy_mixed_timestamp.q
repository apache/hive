create table legacy_table (d timestamp)
stored as parquet;

load data local inpath '../../data/files/parquet_legacy_mixed_timestamps.parq' into table legacy_table;

select * from legacy_table;

drop table legacy_table;