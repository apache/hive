DESCRIBE FUNCTION datetime_legacy_hybrid_calendar;
DESCRIBE FUNCTION EXTENDED datetime_legacy_hybrid_calendar;

SELECT
  '0601-03-07' AS dts,
  CAST('0601-03-07' AS DATE) AS dt,
  datetime_legacy_hybrid_calendar(CAST('0601-03-07' AS DATE)) AS dtp;

SELECT
  '0501-03-07 17:03:00.4321' AS tss,
  CAST('0501-03-07 17:03:00.4321' AS TIMESTAMP) AS ts,
  datetime_legacy_hybrid_calendar(CAST('0501-03-07 17:03:00.4321' AS TIMESTAMP)) AS tsp;

--newer timestamps shouldn't be changed
SELECT
  '1600-03-07 17:03:00.4321' AS tss,
  CAST('1600-03-07 17:03:00.4321' AS TIMESTAMP) AS ts,
  datetime_legacy_hybrid_calendar(CAST('1600-03-07 17:03:00.4321' AS TIMESTAMP)) AS tsp;


--test vectorized UDF--
set hive.fetch.task.conversion=none;

create table datetime_legacy_hybrid_calendar(dt date, ts timestamp) stored as orc;
insert into datetime_legacy_hybrid_calendar values
('0601-03-07', '0501-03-07 17:03:00.4321'),
--post-1582 datetimes shouldn't be changed
('1600-03-07', '1600-03-07 17:03:00.4321');

EXPLAIN
SELECT
  dt, datetime_legacy_hybrid_calendar(dt) AS dtp,
  ts, datetime_legacy_hybrid_calendar(ts) AS tsp
FROM datetime_legacy_hybrid_calendar;

SELECT
  dt, datetime_legacy_hybrid_calendar(dt) AS dtp,
  ts, datetime_legacy_hybrid_calendar(ts) AS tsp
FROM datetime_legacy_hybrid_calendar;

drop table datetime_legacy_hybrid_calendar;
