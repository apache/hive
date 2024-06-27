create table ice_ts_4 (id int, ts timestamp ) stored by iceberg stored as parquet;
insert into ice_ts_4 values (1, cast('2023-07-20 00:00:00' as timestamp));
select readable_metrics from default.ice_ts_4.files;