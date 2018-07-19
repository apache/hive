--! qt:dataset:alltypesorc

SET hive.vectorized.execution.enabled=false;
SET hive.ctas.external.tables=true;
SET hive.external.table.purge.default = true;
CREATE EXTERNAL TABLE druid_table_n2
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
SELECT floor(`__time` to SECOND) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY floor(`__time` to SECOND);

EXPLAIN SELECT floor(`__time` to SECOND) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY floor(`__time` to SECOND);


-- MINUTES
SELECT floor(`__time` to MINUTE) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY floor(`__time` to MINUTE);

EXPLAIN SELECT floor(`__time` to MINUTE) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY floor(`__time` to MINUTE);

-- HOUR
SELECT floor(`__time` to HOUR) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY floor(`__time` to HOUR);

EXPLAIN SELECT floor(`__time` to HOUR) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY floor(`__time` to HOUR);

-- DAY
SELECT EXTRACT(DAY from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(DAY from `__time`);

EXPLAIN SELECT EXTRACT(DAY from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(DAY from `__time`);

--WEEK
SELECT EXTRACT(WEEK from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(WEEK from `__time`);


EXPLAIN SELECT EXTRACT(WEEK from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(WEEK from `__time`);

--MONTH
SELECT EXTRACT(MONTH from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(MONTH from `__time`);

EXPLAIN SELECT EXTRACT(MONTH from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(MONTH from `__time`);

--QUARTER

SELECT EXTRACT(QUARTER from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(QUARTER from `__time`);

EXPLAIN SELECT EXTRACT(QUARTER from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(QUARTER from `__time`);

-- YEAR
SELECT EXTRACT(YEAR from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(YEAR from `__time`);


EXPLAIN SELECT EXTRACT(YEAR from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 GROUP BY EXTRACT(YEAR from `__time`);

-- SELECT WITHOUT GROUP BY

-- SECOND

EXPLAIN SELECT EXTRACT(SECOND from `__time`) FROM druid_table_n2 WHERE EXTRACT(SECOND from `__time`) = 0  LIMIT 1;

SELECT EXTRACT(SECOND from `__time`) FROM druid_table_n2 WHERE EXTRACT(SECOND from `__time`) = 0  LIMIT 1;

-- MINUTE

EXPLAIN SELECT EXTRACT(MINUTE from `__time`) FROM druid_table_n2
WHERE  EXTRACT(MINUTE from `__time`) >= 0 LIMIT 2;

SELECT EXTRACT(MINUTE from `__time`) as minute FROM druid_table_n2
       WHERE  EXTRACT(MINUTE from `__time`) >= 0 order by minute LIMIT 2;
-- HOUR

EXPLAIN SELECT EXTRACT(HOUR from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 LIMIT 1;

SELECT EXTRACT(HOUR from `__time`) FROM druid_table_n2
WHERE character_length(CAST(ctinyint AS STRING)) > 1 AND char_length(CAST(ctinyint AS STRING)) < 10
AND power(cfloat, 2) * pow(csmallint, 3) > 1 AND SQRT(ABS(ctinyint)) > 3 LIMIT 1;

--DAY

EXPLAIN SELECT EXTRACT(DAY from `__time`), EXTRACT(DAY from `__time`) DIV 7 AS WEEK, SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2) AS day_str
FROM druid_table_n2 WHERE SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2)  = 31 LIMIT 1;

SELECT EXTRACT(DAY from `__time`) , EXTRACT(DAY from `__time`) DIV 7 AS WEEK, SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2) AS dar_str
FROM druid_table_n2 WHERE SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 9, 2)  = 31 LIMIT 1 ;

-- WEEK

EXPLAIN SELECT EXTRACT(WEEK from `__time`) FROM druid_table_n2 WHERE EXTRACT(WEEK from `__time`) >= 1
AND  EXTRACT(WEEK from `__time`) DIV 4 + 1 = 1 LIMIT 1;

SELECT EXTRACT(WEEK from `__time`) FROM druid_table_n2 WHERE EXTRACT(WEEK from `__time`) >= 1
AND  EXTRACT(WEEK from `__time`) DIV 4 + 1 = 1 LIMIT 1 ;

--MONTH

EXPLAIN SELECT EXTRACT(MONTH FROM  `__time`) / 4 + 1, EXTRACT(MONTH FROM  `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 6, 2) as month_str FROM druid_table_n2
WHERE EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 AND EXTRACT(MONTH FROM  `__time`) BETWEEN 11 AND 12 LIMIT 1;

SELECT EXTRACT(MONTH FROM  `__time`) / 4 + 1, EXTRACT(MONTH FROM  `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 6, 2) as month_str FROM druid_table_n2
       WHERE EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 AND EXTRACT(MONTH FROM  `__time`) BETWEEN 11 AND 12 LIMIT 1;


--QUARTER

EXPLAIN SELECT EXTRACT(QUARTER from `__time`),  EXTRACT(MONTH FROM  `__time`) / 4 + 1 as q_number FROM druid_table_n2 WHERE EXTRACT(QUARTER from `__time`) >= 4
          AND EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 LIMIT 1;

SELECT EXTRACT(QUARTER from `__time`), EXTRACT(MONTH FROM  `__time`) / 4 + 1  as q_number FROM druid_table_n2 WHERE EXTRACT(QUARTER from `__time`) >= 4
  AND EXTRACT(MONTH FROM  `__time`) / 4 + 1 = 4 LIMIT 1;

--YEAR

EXPLAIN SELECT EXTRACT(YEAR from `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 1, 4) AS year_str FROM druid_table_n2 WHERE EXTRACT(YEAR from `__time`) >= 1969
AND CAST(EXTRACT(YEAR from `__time`) as STRING) = '1969' LIMIT 1;

SELECT EXTRACT(YEAR from `__time`), SUBSTRING(CAST(CAST(`__time` AS DATE) AS STRING), 1, 4) as year_str FROM druid_table_n2 WHERE EXTRACT(YEAR from `__time`) >= 1969
AND CAST(EXTRACT(YEAR from `__time`) as STRING) = '1969' LIMIT 1;


DROP TABLE druid_table_n2;