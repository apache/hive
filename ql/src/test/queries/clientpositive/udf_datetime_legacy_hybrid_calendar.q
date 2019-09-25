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
