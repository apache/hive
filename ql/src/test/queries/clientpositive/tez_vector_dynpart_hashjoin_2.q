--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;

set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.optimize.dynamic.partition.hashjoin=false;

-- Multiple tables, and change the order of the big table (alltypesorc)
-- First try with regular mergejoin
explain
select
  a.*
from
  alltypesorc a,
  src b,
  src c
where
  a.csmallint = cast(b.key as int) and a.csmallint = (cast(c.key as int) + 0)
  and (a.csmallint < 100)
order by a.csmallint, a.ctinyint, a.cint;

select
  a.*
from
  alltypesorc a,
  src b,
  src c
where
  a.csmallint = cast(b.key as int) and a.csmallint = (cast(c.key as int) + 0)
  and (a.csmallint < 100)
order by a.csmallint, a.ctinyint, a.cint;

set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=2000;
set hive.exec.reducers.bytes.per.reducer=200000;
set hive.vectorized.execution.enabled=true;

-- noconditionaltask.size needs to be low enough that entire filtered table results do not fit in one task's hash table
-- Try with dynamically partitioned hash join 
explain
select
  a.*
from
  alltypesorc a,
  src b,
  src c
where
  a.csmallint = cast(b.key as int) and a.csmallint = (cast(c.key as int) + 0)
  and (a.csmallint < 100)
order by a.csmallint, a.ctinyint, a.cint;

select
  a.*
from
  alltypesorc a,
  src b,
  src c
where
  a.csmallint = cast(b.key as int) and a.csmallint = (cast(c.key as int) + 0)
  and (a.csmallint < 100)
order by a.csmallint, a.ctinyint, a.cint;

-- Try different order of tables
explain
select
  a.*
from
  src b,
  alltypesorc a,
  src c
where
  a.csmallint = cast(b.key as int) and a.csmallint = (cast(c.key as int) + 0)
  and (a.csmallint < 100)
order by a.csmallint, a.ctinyint, a.cint;

select
  a.*
from
  src b,
  alltypesorc a,
  src c
where
  a.csmallint = cast(b.key as int) and a.csmallint = (cast(c.key as int) + 0)
  and (a.csmallint < 100)
order by a.csmallint, a.ctinyint, a.cint;
