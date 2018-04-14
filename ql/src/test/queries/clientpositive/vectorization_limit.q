--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

explain vectorization
SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 order by cbigint, cdouble limit 7;
SELECT cbigint, cdouble FROM alltypesorc WHERE cbigint < cdouble and cint > 0 order by cbigint, cdouble limit 7;

set hive.optimize.reducededuplication.min.reducer=1;
set hive.limit.pushdown.memory.usage=0.3f;

-- HIVE-3562 Some limit can be pushed down to map stage - c/p parts from limit_pushdown

explain vectorization detail
select ctinyint,cdouble,csmallint from alltypesorc where ctinyint is not null order by ctinyint,cdouble,csmallint limit 20;
select ctinyint,cdouble,csmallint from alltypesorc where ctinyint is not null order by ctinyint,cdouble,csmallint limit 20;

-- deduped RS
explain vectorization detail
select ctinyint,avg(cdouble + 1) as cavg from alltypesorc group by ctinyint order by ctinyint, cavg limit 20;
select ctinyint,avg(cdouble + 1) as cavg from alltypesorc group by ctinyint order by ctinyint, cavg limit 20;

-- distincts
explain vectorization detail
select distinct(ctinyint) as cdistinct from alltypesorc order by cdistinct limit 20;
select distinct(ctinyint) as cdistinct from alltypesorc order by cdistinct limit 20;

explain vectorization detail
select ctinyint, count(distinct(cdouble)) as count_distinct from alltypesorc group by ctinyint order by ctinyint, count_distinct limit 20;
select ctinyint, count(distinct(cdouble)) as count_distinct from alltypesorc group by ctinyint order by ctinyint, count_distinct limit 20;

-- limit zero
explain vectorization detail
select ctinyint,cdouble from alltypesorc order by ctinyint,cdouble limit 0;
select ctinyint,cdouble from alltypesorc order by ctinyint,cdouble limit 0;

-- 2MR (applied to last RS)
explain vectorization detail
select cdouble, sum(ctinyint) as csum from alltypesorc where ctinyint is not null group by cdouble order by csum, cdouble limit 20;
select cdouble, sum(ctinyint) as csum from alltypesorc where ctinyint is not null group by cdouble order by csum, cdouble limit 20;

