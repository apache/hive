--These files were created by inserting timestamp '2019-01-01 00:30:30.111111111' into column (ts timestamp) where
--writer time zone is Europe/Rome.

--older writer: time zone dependent behavior. convert to reader time zone
create table legacy_table (ts timestamp) stored as avro;

load data local inpath '../../data/files/avro_historical_timestamp_legacy.avro' into table legacy_table;

select * from legacy_table;
--read legacy timestamps as time zone agnostic
set hive.avro.timestamp.skip.conversion=true;
select * from legacy_table;

--newer writer: time zone agnostic behavior. convert to writer time zone (US/Pacific)
create table new_table (ts timestamp) stored as avro;

load data local inpath '../../data/files/avro_historical_timestamp_new.avro' into table new_table;

select * from new_table;