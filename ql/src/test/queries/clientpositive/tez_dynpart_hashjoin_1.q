--! qt:dataset:alltypesorc

-- MASK_STATS

set hive.mapred.mode=nonstrict;

set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.optimize.dynamic.partition.hashjoin=false;

-- First try with regular mergejoin
explain
select
  *
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
order by a.cint;

select
  *
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
order by a.cint;

explain
select
  count(*)
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null;

select
  count(*)
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null;

explain
select
  a.csmallint, count(*) c1
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
group by a.csmallint
order by a.csmallint;

select
  a.csmallint, count(*) c1
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
group by a.csmallint
order by a.csmallint;

set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.exec.reducers.bytes.per.reducer=20000;
set hive.stats.fetch.column.stats=false;
-- Try with dynamically partitioned hashjoin

-- hashjoin with filter
explain select
  *
from alltypesorc a left outer join alltypesorc b on a.cint = b.cint and a.csmallint != a.cint
where
  a.cint between 1000000 and 3000000
order by a.cint;

select
  *
from alltypesorc a left outer join alltypesorc b on a.cint = b.cint and a.csmallint != a.cint
where
  a.cint between 1000000 and 3000000
order by a.cint;

explain
select
  *
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
order by a.cint;

select
  *
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
order by a.cint;

explain
select
  count(*)
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null;

select
  count(*)
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null;

explain
select
  a.csmallint cs, count(*) c1
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
group by a.csmallint
order by cs;

select
  a.csmallint cs, count(*) c1
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
group by a.csmallint
order by cs;
