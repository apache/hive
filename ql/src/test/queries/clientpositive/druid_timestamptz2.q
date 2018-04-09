CREATE database druid_test_dst;
use druid_test_dst;



create table test_base_table(`timecolumn` timestamp, `interval_marker` string, `num_l` double);
insert into test_base_table values ('2015-03-08 00:00:00', 'i1-start', 4);
insert into test_base_table values ('2015-03-08 23:59:59', 'i1-end', 1);
insert into test_base_table values ('2015-03-09 00:00:00', 'i2-start', 4);
insert into test_base_table values ('2015-03-09 23:59:59', 'i2-end', 1);
insert into test_base_table values ('2015-03-10 00:00:00', 'i3-start', 2);
insert into test_base_table values ('2015-03-10 23:59:59', 'i3-end', 2);

CREATE TABLE druid_test_table_1
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "DAY")
AS
select cast(`timecolumn` as timestamp with local time zone) as `__time`, `interval_marker`, `num_l`
FROM druid_test_dst.test_base_table;

select * FROM druid_test_table_1;

CREATE TABLE druid_test_table_2 (`__time` timestamp with local time zone, `interval_marker` string, `num_l` double)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "DAY");


insert into druid_test_table_2 values (cast('2015-03-08 00:00:00' as timestamp with local time zone), 'i1-start', 4);
insert into druid_test_table_2 values (cast('2015-03-08 23:59:59' as timestamp with local time zone), 'i1-end', 1);
insert into druid_test_table_2 values (cast('2015-03-09 00:00:00' as timestamp with local time zone), 'i2-start', 4);
insert into druid_test_table_2 values (cast('2015-03-09 23:59:59' as timestamp with local time zone), 'i2-end', 1);
insert into druid_test_table_2 values (cast('2015-03-10 00:00:00' as timestamp with local time zone), 'i3-start', 2);
insert into druid_test_table_2 values (cast('2015-03-10 23:59:59' as timestamp with local time zone), 'i3-end', 2);

select * FROM druid_test_table_2;

SET TIME ZONE UTC;

CREATE TABLE druid_test_table_utc
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "DAY")
AS
select cast(`timecolumn` as timestamp with local time zone) as `__time`, `interval_marker`, `num_l`
FROM druid_test_dst.test_base_table;

select * FROM druid_test_table_utc;

CREATE TABLE druid_test_table_utc2 (`__time` timestamp with local time zone, `interval_marker` string, `num_l` double)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "DAY");


insert into druid_test_table_utc2 values (cast('2015-03-08 00:00:00' as timestamp with local time zone), 'i1-start', 4);
insert into druid_test_table_utc2 values (cast('2015-03-08 23:59:59' as timestamp with local time zone), 'i1-end', 1);
insert into druid_test_table_utc2 values (cast('2015-03-09 00:00:00' as timestamp with local time zone), 'i2-start', 4);
insert into druid_test_table_utc2 values (cast('2015-03-09 23:59:59' as timestamp with local time zone), 'i2-end', 1);
insert into druid_test_table_utc2 values (cast('2015-03-10 00:00:00' as timestamp with local time zone), 'i3-start', 2);
insert into druid_test_table_utc2 values (cast('2015-03-10 23:59:59' as timestamp with local time zone), 'i3-end', 2);

select * FROM druid_test_table_utc2;
