set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

DROP TABLE IF EXISTS DECIMAL_UDF2_txt;
DROP TABLE IF EXISTS DECIMAL_UDF2;

CREATE TABLE DECIMAL_UDF2_txt (key decimal(20,10), value int)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv7.txt' INTO TABLE DECIMAL_UDF2_txt;

CREATE TABLE DECIMAL_UDF2 (key decimal(20,10), value int)
STORED AS ORC;

INSERT OVERWRITE TABLE DECIMAL_UDF2 SELECT * FROM DECIMAL_UDF2_txt;

EXPLAIN
SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2 WHERE key = 10;

SELECT acos(key), asin(key), atan(key), cos(key), sin(key), tan(key), radians(key)
FROM DECIMAL_UDF2 WHERE key = 10;

EXPLAIN
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

DROP TABLE IF EXISTS DECIMAL_UDF2_txt;
DROP TABLE IF EXISTS DECIMAL_UDF2;
