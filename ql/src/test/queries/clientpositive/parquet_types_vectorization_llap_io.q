-- parquet_types_vectorization.q, re-run with the native Parquet LLAP IO path on. Same schema, same
-- fixture and the same queries, because the point is that the answers do not change: the native
-- path decodes the cached column chunks with the same vectorized column readers, so every type
-- here - including the ones whose projection declines the native path and falls back to the
-- vectorized Parquet reader - has to read back exactly as it does without the flag.
DROP TABLE parquet_types_staging_n4;
DROP TABLE parquet_types_n3;

set hive.mapred.mode=nonstrict;
set hive.llap.io.enabled=true;
set hive.llap.io.parquet.native.enabled=true;

CREATE TABLE parquet_types_staging_n4 (
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

CREATE TABLE parquet_types_n3 (
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

LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_types_staging_n4;

SELECT * FROM parquet_types_staging_n4;

INSERT OVERWRITE TABLE parquet_types_n3
SELECT cint, ctinyint, csmallint, cfloat, cdouble, cstring1, t, cchar, cvarchar,
unhex(cbinary), m1, l1, st1, d FROM parquet_types_staging_n4;

-- test types in group by

EXPLAIN SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  ROUND(AVG(cfloat), 5),
  ROUND(STDDEV_POP(cdouble),5)
FROM parquet_types_n3
GROUP BY ctinyint
ORDER BY ctinyint
;

SELECT ctinyint,
  MAX(cint),
  MIN(csmallint),
  COUNT(cstring1),
  ROUND(AVG(cfloat), 5),
  ROUND(STDDEV_POP(cdouble),5)
FROM parquet_types_n3
GROUP BY ctinyint
ORDER BY ctinyint
;

EXPLAIN SELECT cfloat, count(*) FROM parquet_types_n3 GROUP BY cfloat ORDER BY cfloat;
SELECT cfloat, count(*) FROM parquet_types_n3 GROUP BY cfloat ORDER BY cfloat;

EXPLAIN SELECT cchar, count(*) FROM parquet_types_n3 GROUP BY cchar ORDER BY cchar;
SELECT cchar, count(*) FROM parquet_types_n3 GROUP BY cchar ORDER BY cchar;

EXPLAIN SELECT cvarchar, count(*) FROM parquet_types_n3 GROUP BY cvarchar ORDER BY cvarchar;
SELECT cvarchar, count(*) FROM parquet_types_n3 GROUP BY cvarchar ORDER BY cvarchar;

EXPLAIN SELECT cstring1, count(*) FROM parquet_types_n3 GROUP BY cstring1 ORDER BY cstring1;
SELECT cstring1, count(*) FROM parquet_types_n3 GROUP BY cstring1 ORDER BY cstring1;

EXPLAIN SELECT t, count(*) FROM parquet_types_n3 GROUP BY t ORDER BY t;
SELECT t, count(*) FROM parquet_types_n3 GROUP BY t ORDER BY t;

EXPLAIN SELECT hex(cbinary), count(*) FROM parquet_types_n3 GROUP BY cbinary;
SELECT hex(cbinary), count(*) FROM parquet_types_n3 GROUP BY cbinary;

-- The nested columns, which the native path declines: read here so the flat reads above and the
-- fallback read below hit the same cached file in one session.
SELECT cint, d, m1['k1'], l1[0], st1.c1 FROM parquet_types_n3 ORDER BY cint;

-- Flat again, after the fallback reader has cached chunks of the same file.
SELECT count(*), sum(cint), max(cstring1) FROM parquet_types_n3;

DROP TABLE parquet_types_staging_n4;
DROP TABLE parquet_types_n3;
