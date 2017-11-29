set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=none;

explain vectorization SELECT cbigint, cdouble FROM alltypesparquet WHERE cbigint < cdouble and cint > 0 limit 3,2;
SELECT cbigint, cdouble FROM alltypesparquet WHERE cbigint < cdouble and cint > 0 limit 3,2;

explain vectorization expression
select ctinyint,cdouble,csmallint from alltypesparquet where ctinyint is not null order by ctinyint,cdouble limit 10,3;
select ctinyint,cdouble,csmallint from alltypesparquet where ctinyint is not null order by ctinyint,cdouble limit 10,3;