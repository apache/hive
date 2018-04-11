set hive.druid.broker.address.default=localhost.test;

CREATE EXTERNAL TABLE druid_table_1
STORED BY 'org.apache.hadoop.hive.druid.QTestDruidStorageHandler'
TBLPROPERTIES ("druid.datasource" = "wikipedia");

-- DESCRIBE FORMATTED druid_table_1;

-- GRANULARITY: ALL
EXPLAIN SELECT count(`__time`) from druid_table_1 where `__time` >= '2010-01-01 00:00:00 UTC' AND  `__time` <= '2012-03-01 00:00:00 UTC' OR  added <= 0;

EXPLAIN SELECT count(`__time`) from druid_table_1 where `__time` <= '2010-01-01 00:00:00 UTC';

EXPLAIN
SELECT max(added), sum(variation)
FROM druid_table_1;

-- GRANULARITY: NONE
EXPLAIN
SELECT `__time`, max(added), sum(variation)
FROM druid_table_1
GROUP BY `__time`;

-- GRANULARITY: YEAR
EXPLAIN
SELECT floor_year(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_year(`__time`);

-- @TODO FIXME https://issues.apache.org/jira/browse/CALCITE-2222
-- The current plan of this query is not optimal it can be planned as time series instead of scan
-- GRANULARITY: QUARTER
EXPLAIN
SELECT floor_quarter(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_quarter(`__time`);

-- GRANULARITY: MONTH
EXPLAIN
SELECT floor_month(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_month(`__time`);

-- GRANULARITY: WEEK
EXPLAIN
SELECT floor_week(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_week(`__time`);

-- GRANULARITY: DAY
EXPLAIN
SELECT floor_day(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_day(`__time`);

-- GRANULARITY: HOUR
EXPLAIN
SELECT floor_hour(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_hour(`__time`);

-- GRANULARITY: MINUTE
EXPLAIN
SELECT floor_minute(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_minute(`__time`);

-- GRANULARITY: SECOND
EXPLAIN
SELECT floor_second(`__time`), max(added), sum(variation)
FROM druid_table_1
GROUP BY floor_second(`__time`);

-- WITH FILTER ON DIMENSION
EXPLAIN
SELECT floor_hour(`__time`), max(added), sum(variation)
FROM druid_table_1
WHERE robot='1'
GROUP BY floor_hour(`__time`);

-- WITH FILTER ON TIME
EXPLAIN
SELECT floor_hour(`__time`), max(added), sum(variation)
FROM druid_table_1
WHERE floor_hour(`__time`)
    BETWEEN CAST('2010-01-01 00:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)
        AND CAST('2014-01-01 00:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)
GROUP BY floor_hour(`__time`);

-- WITH FILTER ON TIME
EXPLAIN
SELECT subq.h, subq.m, subq.s
FROM
(
  SELECT floor_hour(`__time`) as h, max(added) as m, sum(variation) as s
  FROM druid_table_1
  GROUP BY floor_hour(`__time`)
) subq
WHERE subq.h BETWEEN CAST('2010-01-01 00:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)
        AND CAST('2014-01-01 00:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE);

-- Simplification of count(__time) as count(*) since time column is not null
EXPLAIN SELECT count(`__time`) from druid_table_1;


EXPLAIN SELECT count(`__time`) from druid_table_1 where `__time` <= '2010-01-01 00:00:00 UTC';

EXPLAIN SELECT count(`__time`) from druid_table_1 where `__time` >= '2010-01-01 00:00:00';


EXPLAIN SELECT count(`__time`) from druid_table_1 where `__time` <= '2010-01-01 00:00:00' OR  `__time` <= '2012-03-01 00:00:00';
