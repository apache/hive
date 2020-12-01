--! qt:dataset:alltypesorc

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled = false;

-- Test timestamp functions in vectorized mode to verify they run correctly end-to-end.
-- Turning on vectorization has been temporarily moved after filling the test table
-- due to bug HIVE-8197.

-- SORT_QUERY_RESULTS

CREATE TABLE alltypesorc_string(cboolean1 boolean, ctimestamp1 timestamp, stimestamp1 string,
    ctimestamp2 timestamp) STORED AS ORC;

INSERT OVERWRITE TABLE alltypesorc_string
SELECT
  cboolean1,
  to_utc_timestamp(ctimestamp1, 'America/Los_Angeles') AS toutc,
  CAST(to_utc_timestamp(ctimestamp1, 'America/Los_Angeles') AS STRING) as cst,
  ctimestamp2
FROM alltypesorc
ORDER BY toutc, cst
LIMIT 40;
INSERT INTO TABLE alltypesorc_string values (false, '2021-09-24 03:18:32.4', '1978-08-05 14:41:05.501', '1999-10-03 16:59:10.396903939');
INSERT INTO TABLE alltypesorc_string values (false, null, '2013-04-10 00:43:46.8547315', null);
INSERT INTO TABLE alltypesorc_string values (false, '2021-09-24 03:18:32.4', null, null);
INSERT INTO TABLE alltypesorc_string values (null, '7160-12-02 06:00:24.81200852', '0004-09-22 18:26:29.519542222', '1966-08-16 13:36:50.183');
INSERT INTO TABLE alltypesorc_string values (null, null, '4966-12-04 09:30:55.202', null);
INSERT INTO TABLE alltypesorc_string values (null, '7160-12-02 06:00:24.81200852', null, null);
INSERT INTO TABLE alltypesorc_string values (true, '1985-07-20 09:30:11.0', '8521-01-16 20:42:05.668832', '1319-02-02 16:31:57.778');
INSERT INTO TABLE alltypesorc_string values (true, null, '1883-04-17 04:14:34.64776', '2024-11-11 16:42:41.101');
INSERT INTO TABLE alltypesorc_string values (true, '0528-10-27 08:15:18.941718273', null, null);

INSERT INTO TABLE alltypesorc_string values
     (false, '2021-09-24 03:18:32.4', '1985-11-18 16:37:54.0', '2010-04-08 02:43:35.861742727'),
     (true, null, '1985-11-18 16:37:54.0', null),
     (null, '2021-09-24 03:18:32.4', null, '1974-10-04 17:21:03.989');

CREATE TABLE alltypesorc_wrong(stimestamp1 string) STORED AS ORC;

INSERT INTO TABLE alltypesorc_wrong SELECT 'abcd' FROM alltypesorc LIMIT 1;
INSERT INTO TABLE alltypesorc_wrong SELECT '2000:01:01 00-00-00' FROM alltypesorc LIMIT 1;
INSERT INTO TABLE alltypesorc_wrong SELECT '0000-00-00 99:99:99' FROM alltypesorc LIMIT 1;

SET hive.vectorized.execution.enabled = true;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  to_unix_timestamp(ctimestamp1) AS c1,
  year(ctimestamp1),
  month(ctimestamp1),
  day(ctimestamp1),
  dayofmonth(ctimestamp1),
  weekofyear(ctimestamp1),
  hour(ctimestamp1),
  minute(ctimestamp1),
  second(ctimestamp1),
  cboolean1,
  ctimestamp1,
  ctimestamp2,
  if (cboolean1, ctimestamp1, timestamp '1319-02-02 16:31:57.778'),
  if (cboolean1, timestamp '2000-12-18 08:42:30.0005', ctimestamp1),
  if (cboolean1, ctimestamp1, ctimestamp2),
  if (cboolean1, ctimestamp1, null),
  if (cboolean1, null, ctimestamp2)
FROM alltypesorc_string
ORDER BY c1;

SELECT
  to_unix_timestamp(ctimestamp1) AS c1,
  year(ctimestamp1),
  month(ctimestamp1),
  day(ctimestamp1),
  dayofmonth(ctimestamp1),
  weekofyear(ctimestamp1),
  hour(ctimestamp1),
  minute(ctimestamp1),
  second(ctimestamp1),
  cboolean1,
  ctimestamp1,
  ctimestamp2,
  if (cboolean1, ctimestamp1, timestamp '1319-02-02 16:31:57.778'),
  if (cboolean1, timestamp '2000-12-18 08:42:30.0005', ctimestamp1),
  if (cboolean1, ctimestamp1, ctimestamp2),
  if (cboolean1, ctimestamp1, null),
  if (cboolean1, null, ctimestamp2)
FROM alltypesorc_string
ORDER BY c1;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  to_unix_timestamp(stimestamp1) AS c1,
  year(stimestamp1),
  month(stimestamp1),
  day(stimestamp1),
  dayofmonth(stimestamp1),
  weekofyear(stimestamp1),
  hour(stimestamp1),
  minute(stimestamp1),
  second(stimestamp1)
FROM alltypesorc_string
ORDER BY c1;

SELECT
  to_unix_timestamp(stimestamp1) AS c1,
  year(stimestamp1),
  month(stimestamp1),
  day(stimestamp1),
  dayofmonth(stimestamp1),
  weekofyear(stimestamp1),
  hour(stimestamp1),
  minute(stimestamp1),
  second(stimestamp1)
FROM alltypesorc_string
ORDER BY c1;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  to_unix_timestamp(ctimestamp1) = to_unix_timestamp(stimestamp1) AS c1,
  year(ctimestamp1) = year(stimestamp1),
  month(ctimestamp1) = month(stimestamp1),
  day(ctimestamp1) = day(stimestamp1),
  dayofmonth(ctimestamp1) = dayofmonth(stimestamp1),
  weekofyear(ctimestamp1) = weekofyear(stimestamp1),
  hour(ctimestamp1) = hour(stimestamp1),
  minute(ctimestamp1) = minute(stimestamp1),
  second(ctimestamp1) = second(stimestamp1)
FROM alltypesorc_string
ORDER BY c1;

-- Should all be true or NULL
SELECT
  to_unix_timestamp(ctimestamp1) = to_unix_timestamp(stimestamp1) AS c1,
  year(ctimestamp1) = year(stimestamp1),
  month(ctimestamp1) = month(stimestamp1),
  day(ctimestamp1) = day(stimestamp1),
  dayofmonth(ctimestamp1) = dayofmonth(stimestamp1),
  weekofyear(ctimestamp1) = weekofyear(stimestamp1),
  hour(ctimestamp1) = hour(stimestamp1),
  minute(ctimestamp1) = minute(stimestamp1),
  second(ctimestamp1) = second(stimestamp1)
FROM alltypesorc_string
ORDER BY c1;

-- Wrong format. Should all be NULL.
EXPLAIN VECTORIZATION EXPRESSION  SELECT
  to_unix_timestamp(stimestamp1) AS c1,
  year(stimestamp1),
  month(stimestamp1),
  day(stimestamp1),
  dayofmonth(stimestamp1),
  weekofyear(stimestamp1),
  hour(stimestamp1),
  minute(stimestamp1),
  second(stimestamp1)
FROM alltypesorc_wrong
ORDER BY c1;

SELECT
  stimestamp1, to_unix_timestamp(stimestamp1) AS c1,
  year(stimestamp1),
  month(stimestamp1),
  day(stimestamp1),
  dayofmonth(stimestamp1),
  weekofyear(stimestamp1),
  hour(stimestamp1),
  minute(stimestamp1),
  second(stimestamp1)
FROM alltypesorc_wrong
ORDER BY c1;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  min(ctimestamp1),
  max(ctimestamp1),
  count(ctimestamp1),
  count(*)
FROM alltypesorc_string;

SELECT
  min(ctimestamp1),
  max(ctimestamp1),
  count(ctimestamp1),
  count(*)
FROM alltypesorc_string;

-- SUM of timestamps are not vectorized reduce-side because they produce a double instead of a long (HIVE-8211)...
EXPLAIN VECTORIZATION EXPRESSION  SELECT
  round(sum(ctimestamp1), 3)
FROM alltypesorc_string;

SELECT
 round(sum(ctimestamp1), 3)
FROM alltypesorc_string;

EXPLAIN VECTORIZATION EXPRESSION  SELECT
  round(avg(ctimestamp1), 0),
  variance(ctimestamp1) between 8.97077295279421E19 and 8.97077295279422E19,
  var_pop(ctimestamp1) between 8.97077295279421E19 and 8.97077295279422E19,
  var_samp(ctimestamp1) between 9.20684592523616E19 and 9.20684592523617E19,
  round(std(ctimestamp1), 3),
  round(stddev(ctimestamp1), 3),
  round(stddev_pop(ctimestamp1), 3),
  round(stddev_samp(ctimestamp1), 3)
FROM alltypesorc_string;

SELECT
  round(avg(ctimestamp1), 0),
  variance(ctimestamp1) between 8.97077295279421E19 and 8.97077295279422E19,
  var_pop(ctimestamp1) between 8.97077295279421E19 and 8.97077295279422E19,
  var_samp(ctimestamp1) between 9.20684592523616E19 and 9.20684592523617E19,
  round(std(ctimestamp1), 3),
  round(stddev(ctimestamp1), 3),
  round(stddev_pop(ctimestamp1), 3),
  round(stddev_samp(ctimestamp1), 3)
FROM alltypesorc_string;
