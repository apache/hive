create table ice_ts_4 (id int, ts timestamp with local time zone) stored by iceberg stored as parquet;
insert into ice_ts_4 values (1, cast('2023-07-20 00:00:00' as timestamp with local time zone));

set hive.fetch.task.conversion=none;
select * from ice_ts_4;

set hive.fetch.task.conversion=more;
select * from ice_ts_4;
