set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.llap.cache.allow.synthetic.fileid=true;
DROP TABLE parquet_types_staging;
DROP TABLE parquet_types;

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
  d date
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
  m1 map<string, varchar(3)>,
  l1 array<int>,
  st1 struct<c1:int, c2:char(1)>,
  d date
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_types_staging;

SELECT * FROM parquet_types_staging;

INSERT OVERWRITE TABLE parquet_types
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
unhex(cbinary), m1, l1, st1, d FROM parquet_types_staging;

SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), m1, l1, st1, d FROM parquet_types;

SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar) FROM parquet_types;

-- test types in group by

SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  ROUND(AVG(cfloat), 5),
  ROUND(STDDEV_POP(cdouble),5)
FROM parquet_types
GROUP BY ctinyint
ORDER BY ctinyint
;

SELECT cfloat, count(*) FROM parquet_types GROUP BY cfloat ORDER BY cfloat;

SELECT cchar, count(*) FROM parquet_types GROUP BY cchar ORDER BY cchar;

SELECT cvarchar, count(*) FROM parquet_types GROUP BY cvarchar ORDER BY cvarchar;

SELECT cstring1, count(*) FROM parquet_types GROUP BY cstring1 ORDER BY cstring1;

SELECT t, count(*) FROM parquet_types GROUP BY t ORDER BY t;

SELECT hex(cbinary), count(*) FROM parquet_types GROUP BY cbinary;