--! qt:dataset:alltypesorc

SET hive.vectorized.execution.enabled=false;
SET hive.ctas.external.tables=true;
SET hive.external.table.purge.default = true;
CREATE EXTERNAL TABLE druid_table
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR", "druid.query.granularity" = "MINUTE")
AS
SELECT cast (`ctimestamp1` as timestamp with local time zone) as `__time`,
  cstring1,
  cstring2,
  cdouble,
  cfloat,
  ctinyint,
  csmallint,
  cint,
  cbigint,
  cboolean1,
  cboolean2
  FROM alltypesorc where ctimestamp1 IS NOT NULL;


-- GROUP BY TIME EXTRACT
--SECONDS
SELECT EXTRACT(SECOND from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(SECOND from `__time`);

EXPLAIN SELECT EXTRACT(SECOND from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(SECOND from `__time`);


-- MINUTES
SELECT EXTRACT(MINUTE from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(MINUTE from `__time`);

EXPLAIN SELECT EXTRACT(MINUTE from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(MINUTE from `__time`);

-- HOUR
SELECT EXTRACT(HOUR from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(HOUR from `__time`);

EXPLAIN SELECT EXTRACT(HOUR from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(HOUR from `__time`);

-- DAY
SELECT EXTRACT(DAY from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(DAY from `__time`);

EXPLAIN SELECT EXTRACT(DAY from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(DAY from `__time`);

--WEEK
SELECT EXTRACT(WEEK from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(WEEK from `__time`);


EXPLAIN SELECT EXTRACT(WEEK from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(WEEK from `__time`);

--MONTH
SELECT EXTRACT(MONTH from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(MONTH from `__time`);

EXPLAIN SELECT EXTRACT(MONTH from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(MONTH from `__time`);

--QUARTER

SELECT EXTRACT(QUARTER from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(QUARTER from `__time`);

EXPLAIN SELECT EXTRACT(QUARTER from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(QUARTER from `__time`);

-- YEAR
SELECT EXTRACT(YEAR from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(YEAR from `__time`);


EXPLAIN SELECT EXTRACT(YEAR from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(YEAR from `__time`);

-- SELECT WITHOUT GROUP BY

-- SECOND

EXPLAIN SELECT EXTRACT(SECOND from `__time`) FROM druid_table WHERE EXTRACT(SECOND from `__time`) = 0  LIMIT 1;

SELECT EXTRACT(SECOND from `__time`) FROM druid_table WHERE EXTRACT(SECOND from `__time`) = 0  LIMIT 1;

-- MINUTE

EXPLAIN SELECT EXTRACT(MINUTE from `__time`) FROM druid_table
WHERE  EXTRACT(MINUTE from `__time`) >= 0 LIMIT 2;

SELECT EXTRACT(MINUTE from `__time`) as minute FROM druid_table
       WHERE  EXTRACT(MINUTE from `__time`) >= 0 order by minute LIMIT 2;
-- HOUR

EXPLAIN SELECT EXTRACT(HOUR from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 LIMIT 1;

SELECT EXTRACT(HOUR from `__time`) FROM druid_table
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 LIMIT 1;

--DAY

EXPLAIN SELECT EXTRACT(DAY from `__time`), EXTRACT(DAY from `__time`) DIV 7 AS WEEK, SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2) AS day_str
FROM druid_table WHERE SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2)  = 31 LIMIT 1;

SELECT EXTRACT(DAY from `__time`) , EXTRACT(DAY from `__time`) DIV 7 AS WEEK, SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2) AS dar_str
FROM druid_table WHERE SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2)  = 31 LIMIT 1 ;

-- WEEK

EXPLAIN SELECT EXTRACT(WEEK from `__time`) FROM druid_table WHERE EXTRACT(WEEK from `__time`) >= 1
AND  EXTRACT(WEEK from `__time`) DIV 4 + 1 = 1 LIMIT 1;

SELECT EXTRACT(WEEK from `__time`) FROM druid_table WHERE EXTRACT(WEEK from `__time`) >= 1
AND  EXTRACT(WEEK from `__time`) DIV 4 + 1 = 1 LIMIT 1 ;

--MONTH

EXPLAIN SELECT EXTRACT(MONTH FROM  `__time`) / 4 + 1, EXTRACT(MONTH FROM  `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 6, 2) as month_str FROM druid_table
WHERE EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 AND EXTRACT(MONTH FROM  `__time`) BETWEEN 11 AND 12 LIMIT 1;

SELECT EXTRACT(MONTH FROM  `__time`) / 4 + 1, EXTRACT(MONTH FROM  `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 6, 2) as month_str FROM druid_table
       WHERE EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 AND EXTRACT(MONTH FROM  `__time`) BETWEEN 11 AND 12 LIMIT 1;


--QUARTER

EXPLAIN SELECT EXTRACT(QUARTER from `__time`),  EXTRACT(MONTH FROM  `__time`) / 4 + 1 as q_number FROM druid_table WHERE EXTRACT(QUARTER from `__time`) >= 4
          AND EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 LIMIT 1;

SELECT EXTRACT(QUARTER from `__time`), EXTRACT(MONTH FROM  `__time`) / 4 + 1  as q_number FROM druid_table WHERE EXTRACT(QUARTER from `__time`) >= 4
  AND EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 LIMIT 1;

--YEAR

EXPLAIN SELECT EXTRACT(YEAR from `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 1, 4) AS year_str FROM druid_table WHERE EXTRACT(YEAR from `__time`) >= 1969
AND CAST(EXTRACT(YEAR from `__time`) as STRING) = '1969' LIMIT 1;

SELECT EXTRACT(YEAR from `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 1, 4) as year_str FROM druid_table WHERE EXTRACT(YEAR from `__time`) >= 1969
AND CAST(EXTRACT(YEAR from `__time`) as STRING) = '1969' LIMIT 1;

-- Cast to Timestamp

explain SELECT CAST(`__time` AS TIMESTAMP) AS `x_time`, SUM(cfloat)  FROM druid_table GROUP BY CAST(`__time` AS TIMESTAMP) ORDER BY `x_time` LIMIT 5;

SELECT CAST(`__time` AS TIMESTAMP) AS `x_time`, SUM(cfloat)  FROM druid_table GROUP BY CAST(`__time` AS TIMESTAMP) ORDER BY `x_time` LIMIT 5;

-- Cast to Date

explain SELECT CAST(`__time` AS DATE) AS `x_date`, SUM(cfloat)  FROM druid_table GROUP BY CAST(`__time` AS DATE) ORDER BY `x_date` LIMIT 5;

SELECT CAST(`__time` AS DATE) AS `x_date`, SUM(cfloat)  FROM druid_table GROUP BY CAST(`__time` AS DATE) ORDER BY `x_date` LIMIT 5;

SELECT CAST(`__time` AS DATE) AS `x_date` FROM druid_table ORDER BY `x_date` LIMIT 5;

-- Test Extract from non datetime column

create table test_extract_from_string_base_table(`timecolumn` timestamp, `date_c` string, `timestamp_c` string,  `metric_c` double);
insert into test_extract_from_string_base_table values ('2015-03-08 00:00:00', '2015-03-10', '2015-03-08 05:30:20', 5.0);

CREATE EXTERNAL TABLE druid_test_extract_from_string_table
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "DAY")
AS select
cast(`timecolumn` as timestamp with local time zone) as `__time`, `date_c`, `timestamp_c`, `metric_c`
FROM test_extract_from_string_base_table;

explain select
year(date_c), month(date_c),day(date_c),
year(timestamp_c), month(timestamp_c),day(timestamp_c), hour(timestamp_c), minute (timestamp_c), second (timestamp_c)
from druid_test_extract_from_string_table;

select year(date_c), month(date_c), day(date_c),
year(timestamp_c), month(timestamp_c), day(timestamp_c), hour(timestamp_c), minute (timestamp_c), second (timestamp_c)
from druid_test_extract_from_string_table;

DROP TABLE druid_test_extract_from_string_table;
DROP TABLE test_extract_from_string_base_table;
DROP TABLE druid_table;