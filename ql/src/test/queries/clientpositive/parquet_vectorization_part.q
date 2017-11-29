set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE alltypesparquet_part(ctinyint tinyint, csmallint smallint, cint int, cbigint bigint, cfloat float, cdouble double, cstring1 string, cstring2 string, ctimestamp1 timestamp, ctimestamp2 timestamp, cboolean1 boolean, cboolean2 boolean) partitioned by (ds string) STORED AS PARQUET;
insert overwrite table alltypesparquet_part partition (ds='2011') select * from alltypesparquet limit 100;
insert overwrite table alltypesparquet_part partition (ds='2012') select * from alltypesparquet limit 100;

select count(cdouble), cint from alltypesparquet_part where ds='2011' group by cint limit 10;
select count(*) from alltypesparquet_part A join alltypesparquet_part B on A.ds=B.ds;
