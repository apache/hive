set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;
set hive.vectorized.use.row.serde.deserialize=false;
set hive.vectorized.use.vector.serde.deserialize=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.mapred.mode=nonstrict;
set hive.llap.cache.allow.synthetic.fileid=true;
set hive.vectorized.groupby.complex.types.enabled=false;
set hive.vectorized.complex.types.enabled=false;

-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS parquet_types_staging;

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

LOAD DATA LOCAL INPATH '../../data/files/parquet_types.txt' OVERWRITE INTO TABLE parquet_types_staging;

-- test various number of projected columns

DROP TABLE IF EXISTS parquet_project_test;

CREATE TABLE parquet_project_test(
cint int,
m1 map<string, string>
) STORED AS PARQUET;

insert into parquet_project_test
select ctinyint, map("color","red") from parquet_types_staging
where ctinyint = 1;

insert into parquet_project_test
select ctinyint, map("color","green") from parquet_types_staging
where ctinyint = 2;

insert into parquet_project_test
select ctinyint, map("color","blue") from parquet_types_staging
where ctinyint = 3;

-- no columns in the projection
explain vectorization select * from parquet_project_test;
select * from parquet_project_test;

-- no columns in the projection, just count(*)
explain vectorization select count(*) from parquet_project_test;
select count(*) from parquet_project_test;

-- project a primitive type
explain vectorization select cint, count(*) from parquet_project_test
group by cint;

select cint, count(*) from parquet_project_test
group by cint;

-- test complex type in projection, this should not get vectorized
explain vectorization select m1["color"], count(*) from parquet_project_test
group by m1["color"];

select m1["color"], count(*) from parquet_project_test
group by m1["color"];


create table if not exists parquet_nullsplit(key string, val string) partitioned by (len string)
stored as parquet;

insert into table parquet_nullsplit partition(len='1')
values ('one', 'red');

explain vectorization select count(*) from parquet_nullsplit where len = '1';
select count(*) from parquet_nullsplit where len = '1';

explain vectorization select count(*) from parquet_nullsplit where len = '99';
select count(*) from parquet_nullsplit where len = '99';

drop table parquet_nullsplit;
drop table parquet_project_test;
drop table parquet_types_staging;
