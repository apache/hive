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
  m1 map<string, varchar(3)>,
  l1 array<int>,
  st1 struct<c1:int, c2:char(1)>
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
  m1 map<string, varchar(3)>,
  l1 array<int>,
  st1 struct<c1:int, c2:char(1)>
) STORED AS PARQUET;

LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_types_staging;

INSERT OVERWRITE TABLE parquet_types SELECT * FROM parquet_types_staging;

SELECT * FROM parquet_types;

SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar) FROM parquet_types;

SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  AVG(cfloat),
  STDDEV_POP(cdouble)
FROM parquet_types
GROUP BY ctinyint
ORDER BY ctinyint
;
