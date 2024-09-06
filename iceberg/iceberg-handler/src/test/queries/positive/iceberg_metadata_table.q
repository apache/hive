-- SORT_QUERY_RESULTS
-- MASK_TIMESTAMP
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

-- Test partitions table
CREATE EXTERNAL TABLE ice_part  (`col1` int, `decimalA` decimal(5,2), `decimalC` decimal(5,2)) PARTITIONED BY SPEC
(decimalC) stored by iceberg tblproperties('format-version'='2');
insert into ice_part values(1, 122.91, 102.21), (1, 12.32, 200.12);
select last_updated_at from default.ice_part.PARTITIONS;
