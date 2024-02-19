--! qt:dataset:src

-- A key is a numeric string
EXPLAIN CBO
SELECT
  ROUND(AVG(key), 3),
  ROUND(STDDEV_POP(key), 3),
  ROUND(STDDEV_SAMP(key), 3),
  ROUND(VAR_POP(key), 3),
  ROUND(VAR_SAMP(key), 3)
FROM src;
SELECT
  ROUND(AVG(key), 3),
  ROUND(STDDEV_POP(key), 3),
  ROUND(STDDEV_SAMP(key), 3),
  ROUND(VAR_POP(key), 3),
  ROUND(VAR_SAMP(key), 3)
FROM src;

-- A value is a non-numeric string
EXPLAIN CBO
SELECT
  ROUND(AVG(value), 3),
  ROUND(STDDEV_POP(value), 3),
  ROUND(STDDEV_SAMP(value), 3),
  ROUND(VAR_POP(value), 3),
  ROUND(VAR_SAMP(value), 3)
FROM src;
SELECT
  ROUND(AVG(value), 3),
  ROUND(STDDEV_POP(value), 3),
  ROUND(STDDEV_SAMP(value), 3),
  ROUND(VAR_POP(value), 3),
  ROUND(VAR_SAMP(value), 3)
FROM src;

EXPLAIN CBO
WITH combined AS (
  SELECT key AS combined_value FROM src
  UNION ALL
  SELECT value AS combined_value FROM src
)
SELECT
  ROUND(AVG(combined_value), 3),
  ROUND(STDDEV_POP(combined_value), 3),
  ROUND(STDDEV_SAMP(combined_value), 3),
  ROUND(VAR_POP(combined_value), 3),
  ROUND(VAR_SAMP(combined_value), 3)
FROM combined;
WITH combined AS (
  SELECT key AS combined_value FROM src
  UNION ALL
  SELECT value AS combined_value FROM src
)
SELECT
  ROUND(AVG(combined_value), 3),
  ROUND(STDDEV_POP(combined_value), 3),
  ROUND(STDDEV_SAMP(combined_value), 3),
  ROUND(VAR_POP(combined_value), 3),
  ROUND(VAR_SAMP(combined_value), 3)
FROM combined;


set hive.cbo.enable=false;

-- A key is a numeric string
SELECT
  ROUND(AVG(key), 3),
  ROUND(STDDEV_POP(key), 3),
  ROUND(STDDEV_SAMP(key), 3),
  ROUND(VAR_POP(key), 3),
  ROUND(VAR_SAMP(key), 3)
FROM src;

-- A value is a non-numeric string
SELECT
  ROUND(AVG(value), 3),
  ROUND(STDDEV_POP(value), 3),
  ROUND(STDDEV_SAMP(value), 3),
  ROUND(VAR_POP(value), 3),
  ROUND(VAR_SAMP(value), 3)
FROM src;

WITH combined AS (
  SELECT key AS combined_value FROM src
  UNION ALL
  SELECT value AS combined_value FROM src
)
SELECT
  ROUND(AVG(combined_value), 3),
  ROUND(STDDEV_POP(combined_value), 3),
  ROUND(STDDEV_SAMP(combined_value), 3),
  ROUND(VAR_POP(combined_value), 3),
  ROUND(VAR_SAMP(combined_value), 3)
FROM combined;
