CREATE TABLE test_udf_configurable (cint1 INT, cint2 INT, ctimestamp TIMESTAMP, text_timestamp STRING);
INSERT INTO test_udf_configurable VALUES
  (10000, 3, CAST('1970-01-01 01:02:03' AS TIMESTAMP), '1970-01-01 01:02:03 4'),
  (20000, 5, CAST('1970-01-02 04:05:06' AS TIMESTAMP), '1970-01-02 04:05:06 5'),
  (30000, 7, CAST('1970-01-03 07:08:09' AS TIMESTAMP), '1970-01-03 07:08:09 6');

set hive.compat=latest;
set hive.strict.timestamp.conversion=false;
set hive.int.timestamp.conversion.in.seconds=true;
set hive.local.time.zone=Asia/Bangkok;
set hive.datetime.formatter=SIMPLE;
set hive.masking.algo=sha512;
set hive.use.googleregex.engine=true;

-- On HiveServer2
SELECT
  -- DECIMAL because of hive.compat=latest
  cint1 / cint2,
  -- Allowed by hive.strict.timestamp.conversion=false
  cint1 = ctimestamp,
  -- Allowed by hive.strict.timestamp.conversion=false
  -- Interpreted as seconds because of hive.int.timestamp.conversion.in.seconds=true
  CAST(cint1 AS TIMESTAMP),
  -- The semantics of "u" is different between SimpleDateFormat and DateTimeFormatter
  DATE_FORMAT(ctimestamp, 'yyyy-MM-dd HH:mm:ss u'),
  FROM_UNIXTIME(cint1, 'yyyy-MM-dd HH:mm:ss u'),
  TO_UNIX_TIMESTAMP(text_timestamp, 'yyyy-MM-dd HH:mm:ss u'),
  UNIX_TIMESTAMP(text_timestamp, 'yyyy-MM-dd HH:mm:ss u'),
  -- SHA512 is used
  MASK_HASH(text_timestamp),
  -- Java's Pattern doesn't support it, then it fails with hive.use.googleregex.engine=false
  text_timestamp RLIKE '\\p{Katakana}+'
FROM test_udf_configurable;

-- On Tez without vectorization
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=false;
SELECT
  -- DECIMAL because of hive.compat=latest
  cint1 / cint2,
  -- Allowed by hive.strict.timestamp.conversion=false
  cint1 = ctimestamp,
  -- Allowed by hive.strict.timestamp.conversion=false
  -- Interpreted as seconds because of hive.int.timestamp.conversion.in.seconds=true
  CAST(cint1 AS TIMESTAMP),
  -- formatter
  DATE_FORMAT(ctimestamp, 'yyyy-MM-dd HH:mm:ss u'),
  FROM_UNIXTIME(cint1, 'yyyy-MM-dd HH:mm:ss u'),
  TO_UNIX_TIMESTAMP(text_timestamp, 'yyyy-MM-dd HH:mm:ss u'),
  UNIX_TIMESTAMP(text_timestamp, 'yyyy-MM-dd HH:mm:ss u'),
  -- SHA512 is used
  MASK_HASH(text_timestamp),
  -- Java's Pattern doesn't support it, then it fails with hive.use.googleregex.engine=false
  text_timestamp RLIKE '\\p{Katakana}+'
FROM test_udf_configurable;
