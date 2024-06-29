-- SORT_QUERY_RESULTS
create table ice_ts_4 (id int, ts timestamp ) stored by iceberg stored as parquet tblproperties ('format-version'='2');
insert into ice_ts_4 values (1, cast('2023-07-20 00:00:00' as timestamp)), (2, cast('2023-07-20 00:00:00' as timestamp));
select * from ice_ts_4;
delete from ice_ts_4 where id = 2;
select * from ice_ts_4;
select readable_metrics from default.ice_ts_4.FILES;
select readable_metrics from default.ice_ts_4.ALL_FILES;
select readable_metrics from default.ice_ts_4.DATA_FILES;
select readable_metrics from default.ice_ts_4.ALL_DATA_FILES;
select readable_metrics from default.ice_ts_4.DELETE_FILES;