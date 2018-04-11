SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;

-- SORT_QUERY_RESULTS

drop table varchar_udf_1;

create table varchar_udf_1 (c1 string, c2 string, c3 varchar(10), c4 varchar(20)) STORED AS ORC;
insert overwrite table varchar_udf_1
  select key, value, key, value from src where key = '238' limit 1;

-- Add a single NULL row that will come from ORC as isRepeated.
insert into varchar_udf_1 values (NULL, NULL, NULL, NULL);

DROP TABLE IF EXISTS DECIMAL_UDF_txt;
DROP TABLE IF EXISTS DECIMAL_UDF;

CREATE TABLE DECIMAL_UDF_txt (key decimal(20,10), value int)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv7.txt' INTO TABLE DECIMAL_UDF_txt;

CREATE TABLE DECIMAL_UDF (key decimal(20,10), value int)
STORED AS ORC;

INSERT OVERWRITE TABLE DECIMAL_UDF SELECT * FROM DECIMAL_UDF_txt;

-- Add a single NULL row that will come from ORC as isRepeated.
insert into DECIMAL_UDF values (NULL, NULL);

drop table if exists count_case_groupby;

create table count_case_groupby (key string, bool boolean) STORED AS orc;
insert into table count_case_groupby values ('key1', true),('key2', false),('key3', NULL),('key4', false),('key5',NULL);

-- Add a single NULL row that will come from ORC as isRepeated.
insert into table count_case_groupby values (NULL, NULL);

set hive.vectorized.adaptor.usage.mode=none;

explain vectorization expression
select
  c2 regexp 'val',
  c4 regexp 'val',
  (c2 regexp 'val') = (c4 regexp 'val')
from varchar_udf_1;

select
  c2 regexp 'val',
  c4 regexp 'val',
  (c2 regexp 'val') = (c4 regexp 'val')
from varchar_udf_1;

explain vectorization expression
select
  regexp_extract(c2, 'val_([0-9]+)', 1),
  regexp_extract(c4, 'val_([0-9]+)', 1),
  regexp_extract(c2, 'val_([0-9]+)', 1) = regexp_extract(c4, 'val_([0-9]+)', 1)
from varchar_udf_1;

select
  regexp_extract(c2, 'val_([0-9]+)', 1),
  regexp_extract(c4, 'val_([0-9]+)', 1),
  regexp_extract(c2, 'val_([0-9]+)', 1) = regexp_extract(c4, 'val_([0-9]+)', 1)
from varchar_udf_1;

explain vectorization expression
select
  regexp_replace(c2, 'val', 'replaced'),
  regexp_replace(c4, 'val', 'replaced'),
  regexp_replace(c2, 'val', 'replaced') = regexp_replace(c4, 'val', 'replaced')
from varchar_udf_1;

select
  regexp_replace(c2, 'val', 'replaced'),
  regexp_replace(c4, 'val', 'replaced'),
  regexp_replace(c2, 'val', 'replaced') = regexp_replace(c4, 'val', 'replaced')
from varchar_udf_1;


set hive.vectorized.adaptor.usage.mode=chosen;

explain vectorization expression
select
  c2 regexp 'val',
  c4 regexp 'val',
  (c2 regexp 'val') = (c4 regexp 'val')
from varchar_udf_1;

select
  c2 regexp 'val',
  c4 regexp 'val',
  (c2 regexp 'val') = (c4 regexp 'val')
from varchar_udf_1;

explain vectorization expression
select
  regexp_extract(c2, 'val_([0-9]+)', 1),
  regexp_extract(c4, 'val_([0-9]+)', 1),
  regexp_extract(c2, 'val_([0-9]+)', 1) = regexp_extract(c4, 'val_([0-9]+)', 1)
from varchar_udf_1;

select
  regexp_extract(c2, 'val_([0-9]+)', 1),
  regexp_extract(c4, 'val_([0-9]+)', 1),
  regexp_extract(c2, 'val_([0-9]+)', 1) = regexp_extract(c4, 'val_([0-9]+)', 1)
from varchar_udf_1;

explain vectorization expression
select
  regexp_replace(c2, 'val', 'replaced'),
  regexp_replace(c4, 'val', 'replaced'),
  regexp_replace(c2, 'val', 'replaced') = regexp_replace(c4, 'val', 'replaced')
from varchar_udf_1;

select
  regexp_replace(c2, 'val', 'replaced'),
  regexp_replace(c4, 'val', 'replaced'),
  regexp_replace(c2, 'val', 'replaced') = regexp_replace(c4, 'val', 'replaced')
from varchar_udf_1;


set hive.vectorized.adaptor.usage.mode=none;

EXPLAIN VECTORIZATION EXPRESSION  SELECT POWER(key, 2) FROM DECIMAL_UDF;

SELECT POWER(key, 2) FROM DECIMAL_UDF;

EXPLAIN VECTORIZATION EXPRESSION
SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF WHERE key = 10;

SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF WHERE key = 10;

set hive.vectorized.adaptor.usage.mode=chosen;

EXPLAIN VECTORIZATION EXPRESSION  SELECT POWER(key, 2) FROM DECIMAL_UDF;

SELECT POWER(key, 2) FROM DECIMAL_UDF;

EXPLAIN VECTORIZATION EXPRESSION
SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF WHERE key = 10;

SELECT
  exp(key), ln(key),
  log(key), log(key, key), log(key, value), log(value, key),
  log10(key), sqrt(key)
FROM DECIMAL_UDF WHERE key = 10;


set hive.vectorized.adaptor.usage.mode=none;

explain vectorization expression
SELECT key, COUNT(CASE WHEN bool THEN 1 WHEN NOT bool THEN 0 ELSE NULL END) AS cnt_bool0_ok FROM count_case_groupby GROUP BY key;

SELECT key, COUNT(CASE WHEN bool THEN 1 WHEN NOT bool THEN 0 ELSE NULL END) AS cnt_bool0_ok FROM count_case_groupby GROUP BY key;

set hive.vectorized.adaptor.usage.mode=chosen;

explain vectorization expression
SELECT key, COUNT(CASE WHEN bool THEN 1 WHEN NOT bool THEN 0 ELSE NULL END) AS cnt_bool0_ok FROM count_case_groupby GROUP BY key;

SELECT key, COUNT(CASE WHEN bool THEN 1 WHEN NOT bool THEN 0 ELSE NULL END) AS cnt_bool0_ok FROM count_case_groupby GROUP BY key;


drop table varchar_udf_1;

DROP TABLE DECIMAL_UDF_txt;
DROP TABLE DECIMAL_UDF;

drop table count_case_groupby;

