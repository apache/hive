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
  `$SUM0`(c_numeric),
  `$SUM0`(CAST(c_numeric AS DOUBLE)),
  `$SUM0`(c_non_numeric),
  `$SUM0`(CAST(c_non_numeric AS DOUBLE)),
  `$SUM0`(c_mix),
  `$SUM0`(CAST(c_mix AS DOUBLE)),

  AVG(c_numeric),
  AVG(CAST(c_numeric AS DOUBLE)),
  AVG(c_non_numeric),
  AVG(CAST(c_non_numeric AS DOUBLE)),
  AVG(c_mix),
  AVG(CAST(c_mix AS DOUBLE)),

  STDDEV_POP(c_numeric),
  STDDEV_POP(CAST(c_numeric AS DOUBLE)),
  STDDEV_POP(c_non_numeric),
  STDDEV_POP(CAST(c_non_numeric AS DOUBLE)),
  STDDEV_POP(c_mix),
  STDDEV_POP(CAST(c_mix AS DOUBLE)),

  STDDEV_SAMP(c_numeric),
  STDDEV_SAMP(CAST(c_numeric AS DOUBLE)),
  STDDEV_SAMP(c_non_numeric),
  STDDEV_SAMP(CAST(c_non_numeric AS DOUBLE)),
  STDDEV_SAMP(c_mix),
  STDDEV_SAMP(CAST(c_mix AS DOUBLE)),

  VAR_POP(c_numeric),
  VAR_POP(CAST(c_numeric AS DOUBLE)),
  VAR_POP(c_non_numeric),
  VAR_POP(CAST(c_non_numeric AS DOUBLE)),
  VAR_POP(c_mix),
  VAR_POP(CAST(c_mix AS DOUBLE)),

  VAR_SAMP(c_numeric),
  VAR_SAMP(CAST(c_numeric AS DOUBLE)),
  VAR_SAMP(c_non_numeric),
  VAR_SAMP(CAST(c_non_numeric AS DOUBLE)),
  VAR_SAMP(c_mix),
  VAR_SAMP(CAST(c_mix AS DOUBLE)),

  -- SUM and COUNT are not converted but used in the transformations
  SUM(c_numeric),
  SUM(CAST(c_numeric AS DOUBLE)),
  SUM(c_non_numeric),
  SUM(CAST(c_non_numeric AS DOUBLE)),
  SUM(c_mix),
  SUM(CAST(c_mix AS DOUBLE)),

  COUNT(c_numeric),
  COUNT(CAST(c_numeric AS DOUBLE)),
  COUNT(c_non_numeric),
  COUNT(CAST(c_non_numeric AS DOUBLE)),
  COUNT(c_mix),
  COUNT(CAST(c_mix AS DOUBLE))
FROM test;

SELECT
  `$SUM0`(c_numeric),
  `$SUM0`(CAST(c_numeric AS DOUBLE)),
  `$SUM0`(c_non_numeric),
  `$SUM0`(CAST(c_non_numeric AS DOUBLE)),
  `$SUM0`(c_mix),
  `$SUM0`(CAST(c_mix AS DOUBLE)),

  AVG(c_numeric),
  AVG(CAST(c_numeric AS DOUBLE)),
  AVG(c_non_numeric),
  AVG(CAST(c_non_numeric AS DOUBLE)),
  AVG(c_mix),
  AVG(CAST(c_mix AS DOUBLE)),

  STDDEV_POP(c_numeric),
  STDDEV_POP(CAST(c_numeric AS DOUBLE)),
  STDDEV_POP(c_non_numeric),
  STDDEV_POP(CAST(c_non_numeric AS DOUBLE)),
  STDDEV_POP(c_mix),
  STDDEV_POP(CAST(c_mix AS DOUBLE)),

  STDDEV_SAMP(c_numeric),
  STDDEV_SAMP(CAST(c_numeric AS DOUBLE)),
  STDDEV_SAMP(c_non_numeric),
  STDDEV_SAMP(CAST(c_non_numeric AS DOUBLE)),
  STDDEV_SAMP(c_mix),
  STDDEV_SAMP(CAST(c_mix AS DOUBLE)),

  VAR_POP(c_numeric),
  VAR_POP(CAST(c_numeric AS DOUBLE)),
  VAR_POP(c_non_numeric),
  VAR_POP(CAST(c_non_numeric AS DOUBLE)),
  VAR_POP(c_mix),
  VAR_POP(CAST(c_mix AS DOUBLE)),

  VAR_SAMP(c_numeric),
  VAR_SAMP(CAST(c_numeric AS DOUBLE)),
  VAR_SAMP(c_non_numeric),
  VAR_SAMP(CAST(c_non_numeric AS DOUBLE)),
  VAR_SAMP(c_mix),
  VAR_SAMP(CAST(c_mix AS DOUBLE)),

  -- SUM and COUNT are not converted but used in the transformations
  SUM(c_numeric),
  SUM(CAST(c_numeric AS DOUBLE)),
  SUM(c_non_numeric),
  SUM(CAST(c_non_numeric AS DOUBLE)),
  SUM(c_mix),
  SUM(CAST(c_mix AS DOUBLE)),

  COUNT(c_numeric),
  COUNT(CAST(c_numeric AS DOUBLE)),
  COUNT(c_non_numeric),
  COUNT(CAST(c_non_numeric AS DOUBLE)),
  COUNT(c_mix),
  COUNT(CAST(c_mix AS DOUBLE))
FROM test;


SET hive.cbo.enable=false;

SELECT
  `$SUM0`(c_numeric),
  `$SUM0`(CAST(c_numeric AS DOUBLE)),
  `$SUM0`(c_non_numeric),
  `$SUM0`(CAST(c_non_numeric AS DOUBLE)),
  `$SUM0`(c_mix),
  `$SUM0`(CAST(c_mix AS DOUBLE)),

  AVG(c_numeric),
  AVG(CAST(c_numeric AS DOUBLE)),
  AVG(c_non_numeric),
  AVG(CAST(c_non_numeric AS DOUBLE)),
  AVG(c_mix),
  AVG(CAST(c_mix AS DOUBLE)),

  STDDEV_POP(c_numeric),
  STDDEV_POP(CAST(c_numeric AS DOUBLE)),
  STDDEV_POP(c_non_numeric),
  STDDEV_POP(CAST(c_non_numeric AS DOUBLE)),
  STDDEV_POP(c_mix),
  STDDEV_POP(CAST(c_mix AS DOUBLE)),

  STDDEV_SAMP(c_numeric),
  STDDEV_SAMP(CAST(c_numeric AS DOUBLE)),
  STDDEV_SAMP(c_non_numeric),
  STDDEV_SAMP(CAST(c_non_numeric AS DOUBLE)),
  STDDEV_SAMP(c_mix),
  STDDEV_SAMP(CAST(c_mix AS DOUBLE)),

  VAR_POP(c_numeric),
  VAR_POP(CAST(c_numeric AS DOUBLE)),
  VAR_POP(c_non_numeric),
  VAR_POP(CAST(c_non_numeric AS DOUBLE)),
  VAR_POP(c_mix),
  VAR_POP(CAST(c_mix AS DOUBLE)),

  VAR_SAMP(c_numeric),
  VAR_SAMP(CAST(c_numeric AS DOUBLE)),
  VAR_SAMP(c_non_numeric),
  VAR_SAMP(CAST(c_non_numeric AS DOUBLE)),
  VAR_SAMP(c_mix),
  VAR_SAMP(CAST(c_mix AS DOUBLE)),

  -- SUM and COUNT are not converted but used in the transformations
  SUM(c_numeric),
  SUM(CAST(c_numeric AS DOUBLE)),
  SUM(c_non_numeric),
  SUM(CAST(c_non_numeric AS DOUBLE)),
  SUM(c_mix),
  SUM(CAST(c_mix AS DOUBLE)),

  COUNT(c_numeric),
  COUNT(CAST(c_numeric AS DOUBLE)),
  COUNT(c_non_numeric),
  COUNT(CAST(c_non_numeric AS DOUBLE)),
  COUNT(c_mix),
  COUNT(CAST(c_mix AS DOUBLE))
FROM test;
