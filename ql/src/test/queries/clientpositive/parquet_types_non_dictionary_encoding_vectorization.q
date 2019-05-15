set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

DROP TABLE parquet_types_staging_n2;
DROP TABLE parquet_types_n1;

set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.use.row.serde.deserialize=true;
set hive.vectorized.use.vector.serde.deserialize=true;
set hive.vectorized.execution.reduce.groupby.enabled = true;

CREATE TABLE parquet_types_staging_n2 (
  cint int,
  ctinyint tinyint,
  csmallint smallint,
  cfloat float,
  cdouble double,
  cstring1 string,
  t timestamp,
  cchar char(5),
  cvarchar varchar(10),
  cbinary string,
  m1 map<string, varchar(3)>,
  l1 array<int>,
  st1 struct<c1:int, c2:char(1)>,
  d date
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

CREATE TABLE parquet_types_n1 (
  cint int,
  ctinyint tinyint,
  csmallint smallint,
  cfloat float,
  cdouble double,
  cstring1 string,
  t timestamp,
  cchar char(5),
  cvarchar varchar(10),
  cbinary binary,
  m1 map<string, varchar(3)>,
  l1 array<int>,
  st1 struct<c1:int, c2:char(1)>,
  d date
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_non_dictionary_types.txt' OVERWRITE INTO TABLE
parquet_types_staging_n2;

SELECT * FROM parquet_types_staging_n2;

INSERT OVERWRITE TABLE parquet_types_n1
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
unhex(cbinary), m1, l1, st1, d FROM parquet_types_staging_n2;

-- test types in group by

EXPLAIN SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  ROUND(AVG(cfloat), 5),
  ROUND(STDDEV_POP(cdouble),5)
FROM parquet_types_n1
GROUP BY ctinyint
ORDER BY ctinyint
;

SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  ROUND(AVG(cfloat), 5),
  ROUND(STDDEV_POP(cdouble),5)
FROM parquet_types_n1
GROUP BY ctinyint
ORDER BY ctinyint
;

EXPLAIN SELECT cfloat, count(*) FROM parquet_types_n1 GROUP BY cfloat ORDER BY cfloat;
SELECT cfloat, count(*) FROM parquet_types_n1 GROUP BY cfloat ORDER BY cfloat;

EXPLAIN SELECT cchar, count(*) FROM parquet_types_n1 GROUP BY cchar ORDER BY cchar;
SELECT cchar, count(*) FROM parquet_types_n1 GROUP BY cchar ORDER BY cchar;

EXPLAIN SELECT cvarchar, count(*) FROM parquet_types_n1 GROUP BY cvarchar ORDER BY cvarchar;
SELECT cvarchar, count(*) FROM parquet_types_n1 GROUP BY cvarchar ORDER BY cvarchar;

EXPLAIN SELECT cstring1, count(*) FROM parquet_types_n1 GROUP BY cstring1 ORDER BY cstring1;
SELECT cstring1, count(*) FROM parquet_types_n1 GROUP BY cstring1 ORDER BY cstring1;

EXPLAIN SELECT hex(cbinary), count(*) FROM parquet_types_n1 GROUP BY cbinary;
SELECT hex(cbinary), count(*) FROM parquet_types_n1 GROUP BY cbinary;