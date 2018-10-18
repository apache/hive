--! qt:dataset:alltypesorc
set hive.fetch.task.conversion=none;
set hive.vectorized.use.row.serde.deserialize=true;
set hive.vectorized.use.vector.serde.deserialize=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;

-- SORT_QUERY_RESULTS

create table if not exists alltypes_parquet_n0 (
  cint int,
  ctinyint tinyint,
  csmallint smallint,
  cfloat float,
  cdouble double,
  cstring1 string) stored as parquet;

insert overwrite table alltypes_parquet_n0
  select cint,
    ctinyint,
    csmallint,
    cfloat,
    cdouble,
    cstring1
  from alltypesorc;

-- test native fileinputformat vectorization

explain vectorization select *
  from alltypes_parquet_n0
  where cint = 528534767
  limit 10;

select *
  from alltypes_parquet_n0
  where cint = 528534767
  limit 10;

explain vectorization select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_parquet_n0
  group by ctinyint;

select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_parquet_n0
  group by ctinyint;

-- exclude MapredParquetInputFormat from vectorization, this should cause mapwork vectorization to be disabled
set hive.vectorized.input.format.excludes=org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;

explain vectorization select *
  from alltypes_parquet_n0
  where cint = 528534767
  limit 10;

select *
  from alltypes_parquet_n0
  where cint = 528534767
  limit 10;

explain vectorization select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_parquet_n0
  group by ctinyint;

select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_parquet_n0
  group by ctinyint;


-- unset hive.vectorized.input.format.excludes and confirm if vectorizer vectorizes the mapwork
set hive.vectorized.input.format.excludes=;

explain vectorization select *
  from alltypes_parquet_n0
  where cint = 528534767
  limit 10;

select *
  from alltypes_parquet_n0
  where cint = 528534767
  limit 10;

explain vectorization select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_parquet_n0
  group by ctinyint;

select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_parquet_n0
  group by ctinyint;


-- check if hive.vectorized.input.format.excludes work with non-parquet inputformats
set hive.vectorized.input.format.excludes=org.apache.hadoop.hive.ql.io.orc.OrcInputFormat,org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
set hive.vectorized.use.row.serde.deserialize=false;
set hive.vectorized.use.vector.serde.deserialize=false;


create table if not exists alltypes_orc_n2 (
  cint int,
  ctinyint tinyint,
  csmallint smallint,
  cfloat float,
  cdouble double,
  cstring1 string) stored as orc;

insert overwrite table alltypes_orc_n2
  select cint,
    ctinyint,
    csmallint,
    cfloat,
    cdouble,
    cstring1
  from alltypesorc;

explain vectorization select *
  from alltypes_orc_n2
  where cint = 528534767
  limit 10;

select *
  from alltypes_orc_n2
  where cint = 528534767
  limit 10;

explain vectorization select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_orc_n2
  group by ctinyint;

select ctinyint,
  max(cint),
  min(csmallint),
  count(cstring1),
  avg(cfloat),
  stddev_pop(cdouble)
  from alltypes_orc_n2
  group by ctinyint;

-- test when input format is excluded row serde is used for vectorization
set hive.vectorized.input.format.excludes=org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat,org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
set hive.vectorized.use.vectorized.input.format=true;
set hive.vectorized.use.row.serde.deserialize=true;
set hive.vectorized.row.serde.inputformat.excludes=;

create table orcTbl (t1 tinyint, t2 tinyint)
stored as orc;

insert into orcTbl values (54, 9), (-104, 25), (-112, 24);

explain vectorization select t1, t2, (t1+t2) from orcTbl where (t1+t2) > 10;

select t1, t2, (t1+t2) from orcTbl where (t1+t2) > 10;

create table parquetTbl (t1 tinyint, t2 tinyint)
stored as parquet;

insert into parquetTbl values (54, 9), (-104, 25), (-112, 24);

explain vectorization SELECT t1, t2, (t1 + t2) FROM parquetTbl WHERE (t1 + t2) > 10;

SELECT t1, t2, (t1 + t2) FROM parquetTbl WHERE (t1 + t2) > 10;
