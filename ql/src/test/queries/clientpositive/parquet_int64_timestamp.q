set hive.parquet.write.int64.timestamp=true;
set time zone Asia/Singapore;

--store timestamps as strings for copying into different schemas
create table parquet_timestamp_staging (i int, s string);

insert into parquet_timestamp_staging
values
(0, '0001-01-01'),
(1, '1677-09-21 00:12:43.145224192'),
(2, '1969-12-31 23:59:59.99999999999999999'),
(3, '1970-01-01 00:00:00'),
(4, '2013-09-27 01:36:18.000000001'),
(5, '2018-01-02 13:14:15.678999'),
(6, '2262-04-11 23:47:16.854775807'),
(7, '9999-12-31 23:59:59.999999999999');

create table parquet_int64_timestamp (i int, ts timestamp) stored as parquet;

--test nanoseconds read/write
set hive.parquet.timestamp.time.unit=nanos;
insert into parquet_int64_timestamp select i, cast (s as timestamp) from parquet_timestamp_staging;
select * from parquet_int64_timestamp order by i;

--test microseconds read/write
set hive.parquet.timestamp.time.unit=micros;
insert into parquet_int64_timestamp select i + 10, cast (s as timestamp) from parquet_timestamp_staging;
select * from parquet_int64_timestamp order by i;

--test milliseconds read/write
set hive.parquet.timestamp.time.unit=millis;
insert into parquet_int64_timestamp select i + 20, cast (s as timestamp) from parquet_timestamp_staging;
select * from parquet_int64_timestamp order by i;


--time zone should not affect values, since timestamp is time zone agnostic
set time zone America/Buenos_Aires;

--test filters
select * from parquet_int64_timestamp where ts >  '1969-12-31 23:59:59.9'
                                            and
                                            ts <  '1970-01-01 00:00:00.0' order by i;
select * from parquet_int64_timestamp where ts <= '1970-01-01 00:00:00.0'
                                            and
                                            ts >= '1970-01-01 00:00:00.0' order by i;
select * from parquet_int64_timestamp where ts =  '1970-01-01 00:00:00.0' order by i;

select * from parquet_int64_timestamp where ts between cast ('1969-12-31 23:59:59.9' as timestamp) and
                                                       cast ('1970-01-01 00:00:00' as timestamp)
                                      order by i;
