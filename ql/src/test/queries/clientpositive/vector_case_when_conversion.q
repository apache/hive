--! qt:dataset:alltypesorc
set hive.stats.fetch.column.stats=true;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

--
-- Test "else string"
--
EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else "none"
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else "none"
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

-- Force usage of VectorUDFAdaptor
set hive.test.vectorized.adaptor.override=true;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else "none"
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else "none"
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

set hive.test.vectorized.adaptor.override=false;



-- Test "else null"
EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else null
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else null
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

-- Force usage of VectorUDFAdaptor
set hive.test.vectorized.adaptor.override=true;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else null
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

SELECT cdouble, cstring1, cint, cfloat, csmallint,
  case
    when (cdouble is not null) then cdouble
    when (cstring1 is not null) then cstring1
    when (cint is not null) then cint
    when (cfloat is not null) then cfloat
    when (csmallint is not null) then csmallint
    else null
    end as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 20;

set hive.test.vectorized.adaptor.override=false;

