set hive.explain.user=false;
set hive.exec.submitviachild=false;
set hive.exec.submit.local.task.via.child=false;

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
