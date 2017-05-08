set hive.mapred.mode=nonstrict;

set hive.explain.user=true;
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
order by c1;

select
  a.csmallint, count(*) c1
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
group by a.csmallint
order by c1;

set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=200000;
set hive.stats.fetch.column.stats=false;
set hive.exec.reducers.bytes.per.reducer=200000;

-- Try with dynamically partitioned hashjoin
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
order by c1;

select
  a.csmallint, count(*) c1
from alltypesorc a join alltypesorc b on a.cint = b.cint
where
  a.cint between 1000000 and 3000000 and b.cbigint is not null
group by a.csmallint
order by c1;

-- Left outer join with residual
explain
select *
from alltypesorc a left outer join alltypesorc b
on a.cint = b.cint or a.csmallint between 1 and 10;
