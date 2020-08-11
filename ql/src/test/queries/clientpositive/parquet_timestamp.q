create table legacy_table (date_test timestamp)
stored as parquet;

load data local inpath '../../data/files/parquet_timestamp.parq' into table legacy_table;

select * from legacy_table;

drop table legacy_table;
