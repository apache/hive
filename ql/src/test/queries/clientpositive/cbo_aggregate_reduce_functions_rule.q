CREATE TABLE test (c_numeric STRING, c_non_numeric STRING, c_mix STRING);
INSERT INTO test VALUES
  ('1', 'val1', '1'),
  ('3', 'val3', 'val3'),
  ('101', 'val101', '101'),
  ('-51', 'val-51', '-51'),
  ('32', 'val32', 'val32'),
  ('0', 'val0', '0'),
  ('87', 'val87', '87'),
  ('55', 'val55', '55');

EXPLAIN CBO
SELECT
  ROUND(`$SUM0`(c_numeric), 3),
  ROUND(`$SUM0`(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(`$SUM0`(c_non_numeric), 3),
  ROUND(`$SUM0`(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(`$SUM0`(c_mix), 3),
  ROUND(`$SUM0`(CAST(c_mix AS DOUBLE)), 3),

  ROUND(AVG(c_numeric), 3),
  ROUND(AVG(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(AVG(c_non_numeric), 3),
  ROUND(AVG(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(AVG(c_mix), 3),
  ROUND(AVG(CAST(c_mix AS DOUBLE)), 3),

  ROUND(STDDEV_POP(c_numeric), 3),
  ROUND(STDDEV_POP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_POP(c_non_numeric), 3),
  ROUND(STDDEV_POP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_POP(c_mix), 3),
  ROUND(STDDEV_POP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(STDDEV_SAMP(c_numeric), 3),
  ROUND(STDDEV_SAMP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_SAMP(c_non_numeric), 3),
  ROUND(STDDEV_SAMP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_SAMP(c_mix), 3),
  ROUND(STDDEV_SAMP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(VAR_POP(c_numeric), 3),
  ROUND(VAR_POP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(VAR_POP(c_non_numeric), 3),
  ROUND(VAR_POP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(VAR_POP(c_mix), 3),
  ROUND(VAR_POP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(VAR_SAMP(c_numeric), 3),
  ROUND(VAR_SAMP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(VAR_SAMP(c_non_numeric), 3),
  ROUND(VAR_SAMP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(VAR_SAMP(c_mix), 3),
  ROUND(VAR_SAMP(CAST(c_mix AS DOUBLE)), 3),

  -- SUM and COUNT are not converted but used in the transformations
  ROUND(SUM(c_numeric), 3),
  ROUND(SUM(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(SUM(c_non_numeric), 3),
  ROUND(SUM(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(SUM(c_mix), 3),
  ROUND(SUM(CAST(c_mix AS DOUBLE)), 3),

  COUNT(c_numeric),
  COUNT(CAST(c_numeric AS DOUBLE)),
  COUNT(c_non_numeric),
  COUNT(CAST(c_non_numeric AS DOUBLE)),
  COUNT(c_mix),
  COUNT(CAST(c_mix AS DOUBLE))
FROM test;

SELECT
  ROUND(`$SUM0`(c_numeric), 3),
  ROUND(`$SUM0`(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(`$SUM0`(c_non_numeric), 3),
  ROUND(`$SUM0`(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(`$SUM0`(c_mix), 3),
  ROUND(`$SUM0`(CAST(c_mix AS DOUBLE)), 3),

  ROUND(AVG(c_numeric), 3),
  ROUND(AVG(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(AVG(c_non_numeric), 3),
  ROUND(AVG(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(AVG(c_mix), 3),
  ROUND(AVG(CAST(c_mix AS DOUBLE)), 3),

  ROUND(STDDEV_POP(c_numeric), 3),
  ROUND(STDDEV_POP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_POP(c_non_numeric), 3),
  ROUND(STDDEV_POP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_POP(c_mix), 3),
  ROUND(STDDEV_POP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(STDDEV_SAMP(c_numeric), 3),
  ROUND(STDDEV_SAMP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_SAMP(c_non_numeric), 3),
  ROUND(STDDEV_SAMP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_SAMP(c_mix), 3),
  ROUND(STDDEV_SAMP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(VAR_POP(c_numeric), 3),
  ROUND(VAR_POP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(VAR_POP(c_non_numeric), 3),
  ROUND(VAR_POP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(VAR_POP(c_mix), 3),
  ROUND(VAR_POP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(VAR_SAMP(c_numeric), 3),
  ROUND(VAR_SAMP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(VAR_SAMP(c_non_numeric), 3),
  ROUND(VAR_SAMP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(VAR_SAMP(c_mix), 3),
  ROUND(VAR_SAMP(CAST(c_mix AS DOUBLE)), 3),

  -- SUM and COUNT are not converted but used in the transformations
  ROUND(SUM(c_numeric), 3),
  ROUND(SUM(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(SUM(c_non_numeric), 3),
  ROUND(SUM(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(SUM(c_mix), 3),
  ROUND(SUM(CAST(c_mix AS DOUBLE)), 3),

  COUNT(c_numeric),
  COUNT(CAST(c_numeric AS DOUBLE)),
  COUNT(c_non_numeric),
  COUNT(CAST(c_non_numeric AS DOUBLE)),
  COUNT(c_mix),
  COUNT(CAST(c_mix AS DOUBLE))
FROM test;


SET hive.cbo.enable=false;

SELECT
  ROUND(`$SUM0`(c_numeric), 3),
  ROUND(`$SUM0`(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(`$SUM0`(c_non_numeric), 3),
  ROUND(`$SUM0`(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(`$SUM0`(c_mix), 3),
  ROUND(`$SUM0`(CAST(c_mix AS DOUBLE)), 3),

  ROUND(AVG(c_numeric), 3),
  ROUND(AVG(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(AVG(c_non_numeric), 3),
  ROUND(AVG(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(AVG(c_mix), 3),
  ROUND(AVG(CAST(c_mix AS DOUBLE)), 3),

  ROUND(STDDEV_POP(c_numeric), 3),
  ROUND(STDDEV_POP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_POP(c_non_numeric), 3),
  ROUND(STDDEV_POP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_POP(c_mix), 3),
  ROUND(STDDEV_POP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(STDDEV_SAMP(c_numeric), 3),
  ROUND(STDDEV_SAMP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_SAMP(c_non_numeric), 3),
  ROUND(STDDEV_SAMP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(STDDEV_SAMP(c_mix), 3),
  ROUND(STDDEV_SAMP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(VAR_POP(c_numeric), 3),
  ROUND(VAR_POP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(VAR_POP(c_non_numeric), 3),
  ROUND(VAR_POP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(VAR_POP(c_mix), 3),
  ROUND(VAR_POP(CAST(c_mix AS DOUBLE)), 3),

  ROUND(VAR_SAMP(c_numeric), 3),
  ROUND(VAR_SAMP(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(VAR_SAMP(c_non_numeric), 3),
  ROUND(VAR_SAMP(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(VAR_SAMP(c_mix), 3),
  ROUND(VAR_SAMP(CAST(c_mix AS DOUBLE)), 3),

  -- SUM and COUNT are not converted but used in the transformations
  ROUND(SUM(c_numeric), 3),
  ROUND(SUM(CAST(c_numeric AS DOUBLE)), 3),
  ROUND(SUM(c_non_numeric), 3),
  ROUND(SUM(CAST(c_non_numeric AS DOUBLE)), 3),
  ROUND(SUM(c_mix), 3),
  ROUND(SUM(CAST(c_mix AS DOUBLE)), 3),

  COUNT(c_numeric),
  COUNT(CAST(c_numeric AS DOUBLE)),
  COUNT(c_non_numeric),
  COUNT(CAST(c_non_numeric AS DOUBLE)),
  COUNT(c_mix),
  COUNT(CAST(c_mix AS DOUBLE))
FROM test;
