set hive.fetch.task.conversion=minimal;

-- Windows-specific test due to space character being escaped in Hive paths on Windows.
-- INCLUDE_OS_WINDOWS

-- Check if vectorization code is handling partitioning on DATE and the other data types.


CREATE TABLE flights_tiny (
  origin_city_name STRING,
  dest_city_name STRING,
  fl_date DATE,
  arr_delay FLOAT,
  fl_num INT
);

LOAD DATA LOCAL INPATH '../../data/files/flights_tiny.txt.1' OVERWRITE INTO TABLE flights_tiny;

CREATE TABLE flights_tiny_orc STORED AS ORC AS
SELECT origin_city_name, dest_city_name, fl_date, to_utc_timestamp(fl_date, 'America/Los_Angeles') as fl_time, arr_delay, fl_num
FROM flights_tiny;

SELECT * FROM flights_tiny_orc;

SET hive.vectorized.execution.enabled=false;

select * from flights_tiny_orc sort by fl_num, fl_date limit 25;

select fl_date, count(*) from flights_tiny_orc group by fl_date;

SET hive.vectorized.execution.enabled=true;

explain
select * from flights_tiny_orc sort by fl_num, fl_date limit 25;

select * from flights_tiny_orc sort by fl_num, fl_date limit 25;

explain
select fl_date, count(*) from flights_tiny_orc group by fl_date;

select fl_date, count(*) from flights_tiny_orc group by fl_date;


SET hive.vectorized.execution.enabled=false;

CREATE TABLE flights_tiny_orc_partitioned_date (
  origin_city_name STRING,
  dest_city_name STRING,
  fl_time TIMESTAMP,
  arr_delay FLOAT,
  fl_num INT
)
PARTITIONED BY (fl_date DATE)
STORED AS ORC;

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE flights_tiny_orc_partitioned_date
PARTITION (fl_date)
SELECT  origin_city_name, dest_city_name, fl_time, arr_delay, fl_num, fl_date
FROM flights_tiny_orc;


select * from flights_tiny_orc_partitioned_date;

select * from flights_tiny_orc_partitioned_date sort by fl_num, fl_date limit 25;

select fl_date, count(*) from flights_tiny_orc_partitioned_date group by fl_date;

SET hive.vectorized.execution.enabled=true;

explain
select * from flights_tiny_orc_partitioned_date;

select * from flights_tiny_orc_partitioned_date;

explain
select * from flights_tiny_orc_partitioned_date sort by fl_num, fl_date limit 25;

select * from flights_tiny_orc_partitioned_date sort by fl_num, fl_date limit 25;

explain
select fl_date, count(*) from flights_tiny_orc_partitioned_date group by fl_date;

select fl_date, count(*) from flights_tiny_orc_partitioned_date group by fl_date;


SET hive.vectorized.execution.enabled=false;

CREATE TABLE flights_tiny_orc_partitioned_timestamp (
  origin_city_name STRING,
  dest_city_name STRING,
  fl_date DATE,
  arr_delay FLOAT,
  fl_num INT
)
PARTITIONED BY (fl_time TIMESTAMP)
STORED AS ORC;

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE flights_tiny_orc_partitioned_timestamp
PARTITION (fl_time)
SELECT  origin_city_name, dest_city_name, fl_date, arr_delay, fl_num, fl_time
FROM flights_tiny_orc;


select * from flights_tiny_orc_partitioned_timestamp;

select * from flights_tiny_orc_partitioned_timestamp sort by fl_num, fl_time limit 25;

select fl_time, count(*) from flights_tiny_orc_partitioned_timestamp group by fl_time;

SET hive.vectorized.execution.enabled=true;

explain
select * from flights_tiny_orc_partitioned_timestamp;

select * from flights_tiny_orc_partitioned_timestamp;

explain
select * from flights_tiny_orc_partitioned_timestamp sort by fl_num, fl_time limit 25;

select * from flights_tiny_orc_partitioned_timestamp sort by fl_num, fl_time limit 25;

explain
select fl_time, count(*) from flights_tiny_orc_partitioned_timestamp group by fl_time;

select fl_time, count(*) from flights_tiny_orc_partitioned_timestamp group by fl_time;
