set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE alltypesparquet_part(ctinyint tinyint, csmallint smallint, cint int, cbigint bigint, cfloat float, cdouble double, cstring1 string, cstring2 string, ctimestamp1 timestamp, ctimestamp2 timestamp, cboolean1 boolean, cboolean2 boolean) partitioned by (ds string) STORED AS PARQUET;
insert overwrite table alltypesparquet_part partition (ds='2011') select * from alltypesparquet order by ctinyint, cint, cbigint limit 100;
insert overwrite table alltypesparquet_part partition (ds='2012') select * from alltypesparquet order by ctinyint, cint, cbigint limit 100;

explain vectorization select (cdouble+2) c1 from alltypesparquet_part order by c1 limit 10;
select (cdouble+2) c1 from alltypesparquet_part order by c1 limit 10;
