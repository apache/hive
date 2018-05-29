set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.llap.cache.allow.synthetic.fileid=true;

DROP TABLE parquet_types_staging_n3;
DROP TABLE parquet_types_n2;
DROP TABLE IF EXISTS parquet_type_nodict;

-- init
CREATE TABLE parquet_types_staging_n3 (
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

CREATE TABLE parquet_types_n2 (
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

LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_types_staging_n3;

INSERT OVERWRITE TABLE parquet_types_n2
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
unhex(cbinary), cdecimal FROM parquet_types_staging_n3;

SET hive.vectorized.execution.enabled=true;

-- select
explain vectorization expression
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_types_n2;

SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_types_n2;

explain vectorization expression
SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_types_n2;

SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_types_n2;

explain vectorization expression
SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  AVG(cfloat),
  STDDEV_POP(cdouble),
  MAX(cdecimal)
FROM parquet_types_n2
GROUP BY ctinyint
ORDER BY ctinyint;

SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  AVG(cfloat),
  STDDEV_POP(cdouble),
  MAX(cdecimal)
FROM parquet_types_n2
GROUP BY ctinyint
ORDER BY ctinyint;

-- test_n10 with dictionary encoding disabled
create table parquet_type_nodict like parquet_types_n2
stored as parquet tblproperties ("parquet.enable.dictionary"="false");

insert into parquet_type_nodict
select * from parquet_types_n2;


explain vectorization expression
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_type_nodict;

SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
hex(cbinary), cdecimal FROM parquet_type_nodict;

explain vectorization expression
SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_type_nodict;

SELECT cchar, LENGTH(cchar), cvarchar, LENGTH(cvarchar), cdecimal, SIGN(cdecimal) FROM parquet_type_nodict;

-- test_n10 timestamp vectorization
explain vectorization select max(t), min(t) from parquet_type_nodict;
select max(t), min(t) from parquet_type_nodict;

-- test_n10 timestamp columnVector isRepeating
create table test_n10 (id int, ts timestamp) stored as parquet tblproperties ("parquet.enable.dictionary"="false");

insert into test_n10 values (1, '2019-01-01 23:12:45.123456'), (2, '2019-01-01 23:12:45.123456'), (3, '2019-01-01 23:12:45.123456');

set hive.fetch.task.conversion=none;
select ts from test_n10 where id > 1;

-- test_n10 null values in timestamp
insert into test_n10 values (3, NULL);
select ts from test_n10 where id > 1;

DROP TABLE parquet_type_nodict;
DROP TABLE test_n10;
