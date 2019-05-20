--! qt:dataset:part
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled = true;
set hive.cli.print.header=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

-- Test timestamp functions in vectorized mode to verify they run correctly end-to-end.

CREATE TABLE date_udf_flight_n0 (
  origin_city_name STRING,
  dest_city_name STRING,
  fl_date DATE,
  arr_delay FLOAT,
  fl_num INT
);
LOAD DATA LOCAL INPATH '../../data/files/flights_tiny.txt.1' OVERWRITE INTO TABLE date_udf_flight_n0;

CREATE TABLE date_udf_flight_orc (
  fl_date DATE,
  fl_time TIMESTAMP
) STORED AS ORC;

INSERT INTO TABLE date_udf_flight_orc SELECT fl_date, to_utc_timestamp(fl_date, 'America/Los_Angeles') FROM date_udf_flight_n0;

SELECT * FROM date_udf_flight_orc;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  fl_time,
  to_unix_timestamp(fl_time),
  year(fl_time),
  month(fl_time),
  day(fl_time),
  dayofmonth(fl_time),
  dayofweek(fl_time),
  weekofyear(fl_time),
  date(fl_time),
  to_date(fl_time),
  date_add(fl_time, 2),
  date_sub(fl_time, 2),
  datediff(fl_time, "2000-01-01"),
  datediff(fl_time, date "2000-01-01"),
  datediff(fl_time, timestamp "2000-01-01 00:00:00"),
  datediff(fl_time, timestamp "2000-01-01 11:13:09"),
  datediff(fl_time, "2007-03-14"),
  datediff(fl_time, date "2007-03-14"),
  datediff(fl_time, timestamp "2007-03-14 00:00:00"),
  datediff(fl_time, timestamp "2007-03-14 08:21:59")
FROM date_udf_flight_orc;

SELECT
  fl_time,
  to_unix_timestamp(fl_time),
  year(fl_time),
  month(fl_time),
  day(fl_time),
  dayofmonth(fl_time),
  dayofweek(fl_time),
  weekofyear(fl_time),
  date(fl_time),
  to_date(fl_time),
  date_add(fl_time, 2),
  date_sub(fl_time, 2),
  datediff(fl_time, "2000-01-01"),
  datediff(fl_time, date "2000-01-01"),
  datediff(fl_time, timestamp "2000-01-01 00:00:00"),
  datediff(fl_time, timestamp "2000-01-01 11:13:09"),
  datediff(fl_time, "2007-03-14"),
  datediff(fl_time, date "2007-03-14"),
  datediff(fl_time, timestamp "2007-03-14 00:00:00"),
  datediff(fl_time, timestamp "2007-03-14 08:21:59")
FROM date_udf_flight_orc;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  fl_date,
  to_unix_timestamp(fl_date),
  year(fl_date),
  month(fl_date),
  day(fl_date),
  dayofmonth(fl_date),
  dayofweek(fl_date),
  weekofyear(fl_date),
  date(fl_date),
  to_date(fl_date),
  date_add(fl_date, 2),
  date_sub(fl_date, 2),
  datediff(fl_date, "2000-01-01"),
  datediff(fl_date, date "2000-01-01"),
  datediff(fl_date, timestamp "2000-01-01 00:00:00"),
  datediff(fl_date, timestamp "2000-01-01 11:13:09"),
  datediff(fl_date, "2007-03-14"),
  datediff(fl_date, date "2007-03-14"),
  datediff(fl_date, timestamp "2007-03-14 00:00:00"),
  datediff(fl_date, timestamp "2007-03-14 08:21:59")
FROM date_udf_flight_orc;

SELECT
  fl_date,
  to_unix_timestamp(fl_date),
  year(fl_date),
  month(fl_date),
  day(fl_date),
  dayofmonth(fl_date),
  dayofweek(fl_date),
  weekofyear(fl_date),
  date(fl_date),
  to_date(fl_date),
  date_add(fl_date, 2),
  date_sub(fl_date, 2),
  datediff(fl_date, "2000-01-01"),
  datediff(fl_date, date "2000-01-01"),
  datediff(fl_date, timestamp "2000-01-01 00:00:00"),
  datediff(fl_date, timestamp "2000-01-01 11:13:09"),
  datediff(fl_date, "2007-03-14"),
  datediff(fl_date, date "2007-03-14"),
  datediff(fl_date, timestamp "2007-03-14 00:00:00"),
  datediff(fl_date, timestamp "2007-03-14 08:21:59")
FROM date_udf_flight_orc;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  fl_time,
  fl_date,
  year(fl_time) = year(fl_date),
  month(fl_time) = month(fl_date),
  day(fl_time) = day(fl_date),
  dayofmonth(fl_time) = dayofmonth(fl_date),
  dayofweek(fl_time) = dayofweek(fl_date),
  weekofyear(fl_time) = weekofyear(fl_date),
  date(fl_time) = date(fl_date),
  to_date(fl_time) = to_date(fl_date),
  date_add(fl_time, 2) = date_add(fl_date, 2),
  date_sub(fl_time, 2) = date_sub(fl_date, 2),
  datediff(fl_time, "2000-01-01") = datediff(fl_date, "2000-01-01"),
  datediff(fl_time, date "2000-01-01") = datediff(fl_date, date "2000-01-01"),
  datediff(fl_time, timestamp "2000-01-01 00:00:00") = datediff(fl_date, timestamp "2000-01-01 00:00:00"),
  datediff(fl_time, timestamp "2000-01-01 11:13:09") = datediff(fl_date, timestamp "2000-01-01 11:13:09"),
  datediff(fl_time, "2007-03-14") = datediff(fl_date, "2007-03-14"),
  datediff(fl_time, date "2007-03-14") = datediff(fl_date, date "2007-03-14"),
  datediff(fl_time, timestamp "2007-03-14 00:00:00") = datediff(fl_date, timestamp "2007-03-14 00:00:00"),
  datediff(fl_time, timestamp "2007-03-14 08:21:59") = datediff(fl_date, timestamp "2007-03-14 08:21:59"),
  datediff(fl_date, "2000-01-01") = datediff(fl_date, date "2000-01-01"),
  datediff(fl_date, "2007-03-14") = datediff(fl_date, date "2007-03-14")
FROM date_udf_flight_orc;

-- Should all be true or NULL
SELECT
  fl_time,
  fl_date,
  year(fl_time) = year(fl_date),
  month(fl_time) = month(fl_date),
  day(fl_time) = day(fl_date),
  dayofmonth(fl_time) = dayofmonth(fl_date),
  dayofweek(fl_time) = dayofweek(fl_date),
  weekofyear(fl_time) = weekofyear(fl_date),
  date(fl_time) = date(fl_date),
  to_date(fl_time) = to_date(fl_date),
  date_add(fl_time, 2) = date_add(fl_date, 2),
  date_sub(fl_time, 2) = date_sub(fl_date, 2),
  datediff(fl_time, "2000-01-01") = datediff(fl_date, "2000-01-01"),
  datediff(fl_time, date "2000-01-01") = datediff(fl_date, date "2000-01-01"),
  datediff(fl_time, timestamp "2000-01-01 00:00:00") = datediff(fl_date, timestamp "2000-01-01 00:00:00"),
  datediff(fl_time, timestamp "2000-01-01 11:13:09") = datediff(fl_date, timestamp "2000-01-01 11:13:09"),
  datediff(fl_time, "2007-03-14") = datediff(fl_date, "2007-03-14"),
  datediff(fl_time, date "2007-03-14") = datediff(fl_date, date "2007-03-14"),
  datediff(fl_time, timestamp "2007-03-14 00:00:00") = datediff(fl_date, timestamp "2007-03-14 00:00:00"),
  datediff(fl_time, timestamp "2007-03-14 08:21:59") = datediff(fl_date, timestamp "2007-03-14 08:21:59"),
  datediff(fl_date, "2000-01-01") = datediff(fl_date, date "2000-01-01"),
  datediff(fl_date, "2007-03-14") = datediff(fl_date, date "2007-03-14")
FROM date_udf_flight_orc;

EXPLAIN VECTORIZATION EXPRESSION  SELECT 
  fl_date, 
  to_date(date_add(fl_date, 2)), 
  to_date(date_sub(fl_date, 2)),
  datediff(fl_date, date_add(fl_date, 2)), 
  datediff(fl_date, date_sub(fl_date, 2)),
  datediff(date_add(fl_date, 2), date_sub(fl_date, 2)) 
FROM date_udf_flight_orc LIMIT 10;

SELECT 
  fl_date, 
  to_date(date_add(fl_date, 2)), 
  to_date(date_sub(fl_date, 2)),
  datediff(fl_date, date_add(fl_date, 2)), 
  datediff(fl_date, date_sub(fl_date, 2)),
  datediff(date_add(fl_date, 2), date_sub(fl_date, 2)) 
FROM date_udf_flight_orc LIMIT 10;

-- Test extracting the date part of expression that includes time
SELECT to_date('2009-07-30 04:17:52') FROM date_udf_flight_orc LIMIT 1;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  min(fl_date) AS c1,
  max(fl_date),
  count(fl_date),
  count(*)
FROM date_udf_flight_orc
ORDER BY c1;

SELECT
  min(fl_date) AS c1,
  max(fl_date),
  count(fl_date),
  count(*)
FROM date_udf_flight_orc
ORDER BY c1;
