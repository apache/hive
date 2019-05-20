--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE TABLE alltypesorc_part_n0(ctinyint tinyint, csmallint smallint, cint int, cbigint bigint, cfloat float, cdouble double, cstring1 string, cstring2 string, ctimestamp1 timestamp, ctimestamp2 timestamp, cboolean1 boolean, cboolean2 boolean) partitioned by (ds string) STORED AS ORC;
insert overwrite table alltypesorc_part_n0 partition (ds='2011') select * from alltypesorc limit 100;
insert overwrite table alltypesorc_part_n0 partition (ds='2012') select * from alltypesorc limit 100;

select count(cdouble), cint from alltypesorc_part_n0 where ds='2011' group by cint limit 10;
select count(*) from alltypesorc_part_n0 A join alltypesorc_part_n0 B on A.ds=B.ds;
