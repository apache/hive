-- SORT_QUERY_RESULTS
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask number of files
--! qt:replace:/(\s+numFiles\s+)\S+(\s+)/$1#Masked#$2/
-- Mask total data files
--! qt:replace:/(\S\"total-data-files\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
set hive.explain.user=false;
create external table ice_parquet_date_transform_year(
  bigintcol bigint,
  intcol integer,
  pcol date
) partitioned by spec (year(pcol))
stored by iceberg;

explain insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-05') values (1234567890123345, 2), (23456789012345678, 4);
insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-05') values (1234567890123345, 2), (23456789012345678, 4);
explain insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
explain insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);
insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);

select * from ice_parquet_date_transform_year;

explain insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date_transform_year;
insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date_transform_year;
explain insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date_transform_year;
insert into table ice_parquet_date_transform_year partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date_transform_year;
describe formatted ice_parquet_date_transform_year;
select * from ice_parquet_date_transform_year;

create external table ice_parquet_date_transform_month(
  bigintcol bigint,
  pcol date,
  intcol integer
) partitioned by spec (month(pcol))
stored by iceberg;

explain insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-31') values (1234567890123345, 2), (23456789012345678, 4);
insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-31') values (1234567890123345, 2), (23456789012345678, 4);
explain insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
explain insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);
insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);

select * from ice_parquet_date_transform_month;

explain insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date_transform_month;
insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date_transform_month;
explain insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date_transform_month;
insert into table ice_parquet_date_transform_month partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date_transform_month;
describe formatted ice_parquet_date_transform_month;
select * from ice_parquet_date_transform_month;

create external table ice_parquet_date_transform_day(
  pcol date,
  bigintcol bigint,
  intcol integer
) partitioned by spec (day(pcol))
stored by iceberg;

explain insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-31') values (1234567890123345, 2), (23456789012345678, 4);
insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-31') values (1234567890123345, 2), (23456789012345678, 4);
explain insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-26') values (1234567890123345, 3), (23456789012345678, 5);
explain insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);
insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-12') values (3456789012345678, 4), (34567890123456789, 6);

select * from ice_parquet_date_transform_day;

explain insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date_transform_day;
insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-13') select bigintcol, intcol from ice_parquet_date_transform_day;
explain insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date_transform_day;
insert into table ice_parquet_date_transform_day partition (pcol = '1999-12-02') select 234675894076895090, intcol from ice_parquet_date_transform_day;
describe formatted ice_parquet_date_transform_day;
select * from ice_parquet_date_transform_day;

create external table ice_parquet_date_transform_truncate(
  pcol string,
  bigintcol bigint,
  intcol integer
) partitioned by spec (truncate(2, pcol))
stored by iceberg;

explain insert into ice_parquet_date_transform_truncate partition (pcol = 'gfhutjkgkd') values (567490276, 6785), (67489376589302, 76859);
insert into ice_parquet_date_transform_truncate partition (pcol = 'gfhutjkgkd') values (567490276, 6785), (67489376589302, 76859);
explain insert into ice_parquet_date_transform_truncate partition (pcol = 'gfhyuitogh') values (567490276, 6785), (67489376589302, 76859);
insert into ice_parquet_date_transform_truncate partition (pcol = 'gfhyuitogh') values (567490276, 6785), (67489376589302, 76859);
explain insert into ice_parquet_date_transform_truncate partition (pcol = 'gfhuiyoprj') select bigintcol, intcol from ice_parquet_date_transform_truncate;
insert into ice_parquet_date_transform_truncate partition (pcol = 'gfhuiyoprj') select bigintcol, intcol from ice_parquet_date_transform_truncate;

describe formatted ice_parquet_date_transform_truncate;
select * from ice_parquet_date_transform_truncate;

create external table ice_parquet_date_transform_bucket(
  pcol string,
  bigintcol bigint,
  intcol integer
) partitioned by spec (bucket(16, pcol))
stored by iceberg;

explain insert into ice_parquet_date_transform_bucket partition (pcol = 'gfhutjkgkd') values (567490276, 6785), (67489376589302, 76859);
insert into ice_parquet_date_transform_bucket partition (pcol = 'gfhutjkgkd') values (567490276, 6785), (67489376589302, 76859);
explain insert into ice_parquet_date_transform_bucket partition (pcol = 'gfhutjkgkd') values (567490276, 6785), (67489376589302, 76859);
insert into ice_parquet_date_transform_bucket partition (pcol = 'gfhutjkgkd') values (567490276, 6785), (67489376589302, 76859);
explain insert into ice_parquet_date_transform_bucket partition (pcol = 'gfhutjkgkd') select bigintcol, intcol from ice_parquet_date_transform_bucket;
insert into ice_parquet_date_transform_bucket partition (pcol = 'gfhutjkgkd') select bigintcol, intcol from ice_parquet_date_transform_bucket;

describe formatted ice_parquet_date_transform_bucket;
select * from ice_parquet_date_transform_bucket;

create external table ice_parquet_decimal_transform_bucket(
  pcol decimal(38, 0)
) partitioned by spec (bucket(16, pcol))
stored by iceberg;

explain insert into ice_parquet_decimal_transform_bucket values
('0'),
('50000000000000000000441610525');
insert into ice_parquet_decimal_transform_bucket values
('0'),
('50000000000000000000441610525');

describe formatted ice_parquet_decimal_transform_bucket;
select * from ice_parquet_decimal_transform_bucket;

drop table ice_parquet_date_transform_year;
drop table ice_parquet_date_transform_month;
drop table ice_parquet_date_transform_day;
drop table ice_parquet_date_transform_truncate;
drop table ice_parquet_date_transform_bucket;
drop table ice_parquet_decimal_transform_bucket;
