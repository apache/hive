set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS DECIMAL_UDF2_txt;
DROP TABLE IF EXISTS DECIMAL_UDF2;

CREATE TABLE DECIMAL_UDF2_txt (key decimal(14,5), value int)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv7.txt' INTO TABLE DECIMAL_UDF2_txt;

CREATE TABLE DECIMAL_UDF2 (key decimal(14,5), value int)
STORED AS ORC;

INSERT OVERWRITE TABLE DECIMAL_UDF2 SELECT * FROM DECIMAL_UDF2_txt;

-- Add a single NULL row that will come from ORC as isRepeated.
insert into DECIMAL_UDF2 values (NULL, NULL);

EXPLAIN VECTORIZATION DETAIL
SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2 WHERE key = 10;

SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2 WHERE key = 10;

SELECT SUM(HASH(*))
FROM (SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2) q;

EXPLAIN VECTORIZATION DETAIL
SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF2 WHERE key = 10;

SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF2 WHERE key = 10;

SELECT SUM(HASH(*))
FROM (SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF2) q;

-- DECIMAL_64

EXPLAIN VECTORIZATION DETAIL
SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2_txt WHERE key = 10;

SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2_txt WHERE key = 10;

SELECT SUM(HASH(*))
FROM (SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2_txt) q;

EXPLAIN VECTORIZATION DETAIL
SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF2_txt WHERE key = 10;

SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF2_txt WHERE key = 10;

SELECT SUM(HASH(*))
FROM (SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF2_txt) q;

DROP TABLE IF EXISTS DECIMAL_UDF2_txt;
DROP TABLE IF EXISTS DECIMAL_UDF2;
