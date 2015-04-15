SET hive.vectorized.execution.enabled=true;

DROP TABLE parquet_types_staging;
DROP TABLE parquet_types;

-- init
CREATE TABLE parquet_types_staging (
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
  d date,
  cdecimal decimal(4,2)
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

CREATE TABLE parquet_types (
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
  cdecimal decimal(4,2)
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_types_staging;

INSERT OVERWRITE TABLE parquet_types
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
unhex(cbinary), cdecimal FROM parquet_types_staging;

-- select
explain
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_types;

SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_types;

explain
SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_types;

SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_types;

explain
SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  AVG(cfloat),
  STDDEV_POP(cdouble),
  MAX(cdecimal)
FROM parquet_types
GROUP BY ctinyint
ORDER BY ctinyint;

SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  AVG(cfloat),
  STDDEV_POP(cdouble),
  MAX(cdecimal)
FROM parquet_types
GROUP BY ctinyint
ORDER BY ctinyint;