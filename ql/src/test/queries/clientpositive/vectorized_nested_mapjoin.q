set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

explain vectorization select sum(t1.td) from (select  v1.csmallint as tsi, v1.cdouble as td from alltypesorc v1, alltypesorc v2 where v1.ctinyint=v2.ctinyint) t1 join alltypesorc v3 on t1.tsi=v3.csmallint;

select sum(t1.td) from (select  v1.csmallint as tsi, v1.cdouble as td from alltypesorc v1, alltypesorc v2 where v1.ctinyint=v2.ctinyint) t1 join alltypesorc v3 on t1.tsi=v3.csmallint;
