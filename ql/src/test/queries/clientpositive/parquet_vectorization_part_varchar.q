set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE alltypesparquet_part_varchar(ctinyint tinyint, csmallint smallint, cint int, cbigint bigint, cfloat float, cdouble double, cstring1 string, cstring2 string, ctimestamp1 timestamp, ctimestamp2 timestamp, cboolean1 boolean, cboolean2 boolean) partitioned by (ds varchar(4)) STORED AS PARQUET;
insert overwrite table alltypesparquet_part_varchar partition (ds='2011') select * from alltypesparquet limit 100;
insert overwrite table alltypesparquet_part_varchar partition (ds='2012') select * from alltypesparquet limit 100;

select count(cdouble), cint from alltypesparquet_part_varchar where ds='2011' group by cint limit 10;
select count(*) from alltypesparquet_part_varchar A join alltypesparquet_part_varchar B on A.ds=B.ds;
