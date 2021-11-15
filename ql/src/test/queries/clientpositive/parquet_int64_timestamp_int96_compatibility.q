--store timestamps as strings for copying into different schemas
create table parquet_timestamp_staging_2 (i int, s string);
insert into parquet_timestamp_staging_2
values
(0, '0001-01-01'),
(1, '1677-09-21 00:12:43.145224192'),
(2, '1969-12-31 23:59:59.99999999999999999'),
(3, '1970-01-01 00:00:00'),
(4, '2013-09-27 01:36:18.000000001'),
(5, '2018-01-02 13:14:15.678999'),
(6, '2262-04-11 23:47:16.854775807'),
(7, '9999-12-31 23:59:59.999999999999');


--make int64 table with microsecond granularity
set hive.parquet.write.int64.timestamp=true;
set hive.parquet.timestamp.time.unit=micros;
create table parquet_int64 (i int, ts timestamp) stored as parquet;
insert into parquet_int64 select i, cast (s as timestamp) from parquet_timestamp_staging_2;

--make int96 table
set hive.parquet.write.int64.timestamp=false;
create table parquet_int96 (i int, ts timestamp) stored as parquet;
insert into parquet_int96 select i + 10, cast (s as timestamp) from parquet_timestamp_staging_2;

--join int64 and int96 tables
select parquet_int64.i, parquet_int64.ts, parquet_int96.ts
    from parquet_int64
    join parquet_int96
    on parquet_int64.ts = parquet_int96.ts
    order by parquet_int64.i;


--create table with mixed int64/milli and int96 values
set hive.parquet.write.int64.timestamp=true;
set hive.parquet.timestamp.time.unit=millis;
create table parquet_mixed_timestamp as select * from parquet_int64;
set hive.parquet.write.int64.timestamp=false;
insert into parquet_mixed_timestamp select i + 10, cast (s as timestamp) from parquet_timestamp_staging_2;

select * from parquet_mixed_timestamp order by i;
select * from parquet_mixed_timestamp where ts > cast ('2200-01-01 00:00:00.00' as timestamp) order by i;
select * from parquet_mixed_timestamp where ts < cast ('1900-12-31 23:59:59.9999999999' as timestamp) order by i;
select count(*) from parquet_mixed_timestamp where ts = cast ('1970-01-01 00:00:00.00' as timestamp);
--join mixed table and int64 table
select *
    from parquet_mixed_timestamp
    join parquet_int64
    on parquet_mixed_timestamp.ts = parquet_int64.ts
    order by parquet_mixed_timestamp.i;
