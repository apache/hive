set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled = true
;
explain vectorization expression
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else "c"
  end,
  case csmallint
    when 418 then "a"
    when 12205 then "b"
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else "c"
  end,
  case csmallint
    when 418 then "a"
    when 12205 then "b"
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
explain vectorization expression
select 
  csmallint,
  case 
    when csmallint = 418 then "a"
    when csmallint = 12205 then "b"
    else null
  end,
  case csmallint
    when 418 then "a"
    when 12205 then null
    else "c"
  end
from alltypesorc
where csmallint = 418
or csmallint = 12205
or csmallint = 10583
;
explain vectorization expression
select 
  sum(case when cint % 2 = 0 then 1 else 0 end) as ceven,
  sum(case when cint % 2 = 1 then 1 else 0 end) as codd
from alltypesorc;
select 
  sum(case when cint % 2 = 0 then 1 else 0 end) as ceven,
  sum(case when cint % 2 = 1 then 1 else 0 end) as codd
from alltypesorc;
explain vectorization expression
select 
  sum(case when cint % 2 = 0 then cint else 0 end) as ceven,
  sum(case when cint % 2 = 1 then cint else 0 end) as codd
from alltypesorc;
select 
  sum(case when cint % 2 = 0 then cint else 0 end) as ceven,
  sum(case when cint % 2 = 1 then cint else 0 end) as codd
from alltypesorc;
