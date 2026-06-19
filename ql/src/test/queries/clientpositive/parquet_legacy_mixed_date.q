create table legacy_table (d date)
stored as parquet;

load data local inpath '../../data/files/parquet_legacy_mixed_dates.parq' into table legacy_table;

select * from legacy_table;

set hive.parquet.date.proleptic.gregorian.default=true;

select * from legacy_table;

drop table legacy_table;