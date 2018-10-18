--! qt:dataset:alltypesorc
set hive.explain.user=false;
set hive.exec.submitviachild=false;
set hive.exec.submit.local.task.via.child=false;
set hive.llap.cache.allow.synthetic.fileid=true;

create table if not exists alltypes_parquet (
  cint int, 
  ctinyint tinyint, 
  csmallint smallint, 
  cfloat float, 
  cdouble double, 
  cstring1 string) stored as parquet;
  
insert overwrite table alltypes_parquet 
  select cint, 
    ctinyint, 
    csmallint, 
    cfloat, 
    cdouble, 
    cstring1 
  from alltypesorc;
  
SET hive.vectorized.execution.enabled=true;
  
explain vectorization select * 
  from alltypes_parquet
  where cint = 528534767 
  limit 10;
select * 
  from alltypes_parquet
  where cint = 528534767 
  limit 10;

explain vectorization select ctinyint, 
  max(cint), 
  min(csmallint), 
  count(cstring1), 
  avg(cfloat), 
  stddev_pop(cdouble)
  from alltypes_parquet
  group by ctinyint;
select ctinyint, 
  max(cint), 
  min(csmallint), 
  count(cstring1), 
  avg(cfloat), 
  stddev_pop(cdouble)
  from alltypes_parquet
  group by ctinyint;

CREATE TABLE empty_parquet(x int) PARTITIONED BY (y int) stored as parquet;
select * from empty_parquet t1 join empty_parquet t2 where t1.x=t2.x;
