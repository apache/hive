--! qt:dataset:alltypesorc
set hive.stats.fetch.column.stats=true;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cdouble, cstring1, cint, cfloat, csmallint, coalesce(cdouble, cstring1, cint, cfloat, csmallint) as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 10;

SELECT cdouble, cstring1, cint, cfloat, csmallint, coalesce(cdouble, cstring1, cint, cfloat, csmallint) as c 
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT ctinyint, cdouble, cint, coalesce(ctinyint+10, (cdouble+log2(cint)), 0) as c
FROM alltypesorc
WHERE (ctinyint IS NULL)
ORDER BY ctinyint, cdouble, cint, c
LIMIT 10;

SELECT ctinyint, cdouble, cint, coalesce(ctinyint+10, (cdouble+log2(cint)), 0) as c
FROM alltypesorc
WHERE (ctinyint IS NULL)
ORDER BY ctinyint, cdouble, cint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cfloat, cbigint, coalesce(cfloat, cbigint, 0) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

SELECT cfloat, cbigint, coalesce(cfloat, cbigint, 0) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT ctimestamp1, ctimestamp2, coalesce(ctimestamp1, ctimestamp2) as c
FROM alltypesorc 
WHERE ctimestamp1 IS NOT NULL OR ctimestamp2 IS NOT NULL
ORDER BY ctimestamp1, ctimestamp2, c
LIMIT 10;

SELECT ctimestamp1, ctimestamp2, coalesce(ctimestamp1, ctimestamp2) as c
FROM alltypesorc 
WHERE ctimestamp1 IS NOT NULL OR ctimestamp2 IS NOT NULL
ORDER BY ctimestamp1, ctimestamp2, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cfloat, cbigint, coalesce(cfloat, cbigint) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

SELECT cfloat, cbigint, coalesce(cfloat, cbigint) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cbigint, ctinyint, coalesce(cbigint, ctinyint) as c
FROM alltypesorc
WHERE cbigint IS NULL
LIMIT 10;

SELECT cbigint, ctinyint, coalesce(cbigint, ctinyint) as c
FROM alltypesorc
WHERE cbigint IS NULL
LIMIT 10;

-- HIVE-27632, check vector coalesce with decimal in filter
create database test;
use test;
 CREATE EXTERNAL TABLE test.ext_1(
   ord_key string,
   col2 string,
   col3 decimal(18,6),
   col4 String,
   col5 decimal(18,6))
   CLUSTERED BY (
   ord_key)
 SORTED BY (
   ord_key ASC)
 INTO 64 BUCKETS
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 TBLPROPERTIES (
   'TRANSLATED_TO_EXTERNAL'='TRUE',
   'external.table.purge'='TRUE',
   'orc.output.codec'='snappy');


 CREATE EXTERNAL TABLE test.ext_2(
   Col1 string,
   col2 string,
   Col3 decimal(18,6),
   col4 string,
   col5 string)
 CLUSTERED BY (
   col2)
 SORTED BY (
   col2 ASC)
 INTO 64 BUCKETS
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 TBLPROPERTIES (
   'TRANSLATED_TO_EXTERNAL'='TRUE',
   'external.table.purge'='TRUE',
   'orc.output.codec'='snappy');

insert into test.ext_1 (col2) values('a');
insert into test.ext_2 (col2) values ('a');
select ord_key,test1.col5 FROM test.ext_1 test1
left outer join test.ext_2 test2 on (test1.col4 = test2.col4)
where (NVL(test1.col5, 0) = 0) limit 10;
