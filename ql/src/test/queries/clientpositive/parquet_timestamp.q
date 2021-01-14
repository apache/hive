--! qt:timezone:Asia/Singapore

create table legacy_table_parq1 (date_test timestamp)
stored as parquet;

load data local inpath '../../data/files/tbl_parq1/' into table legacy_table_parq1;

select * from legacy_table_parq1;

set hive.parquet.timestamp.legacy.conversion.enabled=false;

select * from legacy_table_parq1;

set hive.parquet.timestamp.legacy.conversion.enabled=true;
set hive.vectorized.execution.enabled=false;

select * from legacy_table_parq1;

set hive.parquet.timestamp.legacy.conversion.enabled=false;

select * from legacy_table_parq1;

drop table legacy_table_parq1;
