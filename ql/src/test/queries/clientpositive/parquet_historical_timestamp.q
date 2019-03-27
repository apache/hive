--These files were created by inserting timestamp '2019-01-01 00:30:30.111111111' where writer time zone is Europe/Rome.

--older writer: time zone dependent behavior. convert to reader time zone
create table legacy_table (t timestamp) stored as parquet;

load data local inpath '../../data/files/parquet_historical_timestamp_legacy.parq' into table legacy_table;

select * from legacy_table;


--newer writer: time zone agnostic behavior. convert to writer time zone
create table new_table (t timestamp) stored as parquet;

load data local inpath '../../data/files/parquet_historical_timestamp_new.parq' into table new_table;

select * from new_table;