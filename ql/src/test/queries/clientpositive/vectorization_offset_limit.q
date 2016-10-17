set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.mapred.mode=nonstrict;

explain SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 limit 3,2;
SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 limit 3,2;

explain
select ctinyint,cdouble,csmallint from alltypesorc where ctinyint is not null order by ctinyint,cdouble limit 10,3;
select ctinyint,cdouble,csmallint from alltypesorc where ctinyint is not null order by ctinyint,cdouble limit 10,3;