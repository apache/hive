set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.llap.cache.allow.synthetic.fileid=true;

DROP TABLE parquet_types_staging;
DROP TABLE parquet_types;
DROP TABLE IF EXISTS parquet_type_nodict;

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

SET hive.vectorized.execution.enabled=true;

-- select
explain vectorization expression
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_types;

SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_types;

explain vectorization expression
SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_types;

SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_types;

explain vectorization expression
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

-- test with dictionary encoding disabled
create table parquet_type_nodict like parquet_types
stored as parquet tblproperties ("parquet.enable.dictionary"="false");

insert into parquet_type_nodict
select * from parquet_types;


explain vectorization expression
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_type_nodict;

SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_type_nodict;

explain vectorization expression
SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_type_nodict;

SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_type_nodict;

-- test timestamp vectorization
explain vectorization select max(t), min(t) from parquet_type_nodict;
select max(t), min(t) from parquet_type_nodict;

-- test timestamp columnVector isRepeating
create table test (id int, ts timestamp) stored as parquet tblproperties ("parquet.enable.dictionary"="false");

insert into test values (1, '2019-01-01 23:12:45.123456'), (2, '2019-01-01 23:12:45.123456'), (3, '2019-01-01 23:12:45.123456');

set hive.fetch.task.conversion=none;
select ts from test where id > 1;

-- test null values in timestamp
insert into test values (3, NULL);
select ts from test where id > 1;

DROP TABLE parquet_type_nodict;
DROP TABLE test;
