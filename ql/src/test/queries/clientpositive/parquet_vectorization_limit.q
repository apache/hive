--! qt:dataset:alltypesparquet
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

explain vectorization SELECT cbigint, cdouble FROM alltypesparquet WHERE cbigint < cdouble AND cint > 0 ORDER BY cbigint, cdouble LIMIT 7;
SELECT cbigint, cdouble FROM alltypesparquet WHERE cbigint < cdouble AND cint > 0 ORDER BY cbigint, cdouble LIMIT 7;

set hive.optimize.reducededuplication.min.reducer=1;
set hive.limit.pushdown.memory.usage=0.3f;

-- HIVE-3562 Some limit can be pushed down to map stage - c/p parts from limit_pushdown

explain VECTORIZATION EXPRESSION
select ctinyint,cdouble,csmallint from alltypesparquet where ctinyint is not null order by ctinyint,cdouble,csmallint limit 20;
select ctinyint,cdouble,csmallint from alltypesparquet where ctinyint is not null order by ctinyint,cdouble,csmallint limit 20;

-- deduped RS
explain VECTORIZATION EXPRESSION
select ctinyint,avg(cdouble + 1) from alltypesparquet group by ctinyint order by ctinyint limit 20;
select ctinyint,avg(cdouble + 1) from alltypesparquet group by ctinyint order by ctinyint limit 20;

-- distincts
explain VECTORIZATION EXPRESSION
select distinct(ctinyint) from alltypesparquet order by ctinyint limit 20;
select distinct(ctinyint) from alltypesparquet order by ctinyint limit 20;

explain VECTORIZATION EXPRESSION
select ctinyint, count(distinct(cdouble)) from alltypesparquet group by ctinyint order by ctinyint limit 20;
select ctinyint, count(distinct(cdouble)) from alltypesparquet group by ctinyint order by ctinyint limit 20;

-- limit zero
explain VECTORIZATION EXPRESSION
select ctinyint,cdouble from alltypesparquet order by ctinyint,cdouble limit 0;
select ctinyint,cdouble from alltypesparquet order by ctinyint,cdouble limit 0;

-- 2MR (applied to last RS)
explain VECTORIZATION EXPRESSION
select cdouble, sum(ctinyint) as sum from alltypesparquet where ctinyint is not null group by cdouble order by sum, cdouble limit 20;
select cdouble, sum(ctinyint) as sum from alltypesparquet where ctinyint is not null group by cdouble order by sum, cdouble limit 20;

