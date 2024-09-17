--! qt:dataset:alltypesorc
set hive.stats.fetch.column.stats=true;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cdouble, cstring1, cint, cfloat, csmallint, coalesce(cdouble, cstring1, cint, cfloat, csmallint) as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 10;

SELECT cdouble, cstring1, cint, cfloat, csmallint, coalesce(cdouble, cstring1, cint, cfloat, csmallint) as c
FROM alltypesorc
WHERE (cdouble IS NULL)
ORDER BY cdouble, cstring1, cint, cfloat, csmallint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT ctinyint, cdouble, cint, coalesce(ctinyint+10, (cdouble+log2(cint)), 0) as c
FROM alltypesorc
WHERE (ctinyint IS NULL)
ORDER BY ctinyint, cdouble, cint, c
LIMIT 10;

SELECT ctinyint, cdouble, cint, coalesce(ctinyint+10, (cdouble+log2(cint)), 0) as c
FROM alltypesorc
WHERE (ctinyint IS NULL)
ORDER BY ctinyint, cdouble, cint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cfloat, cbigint, coalesce(cfloat, cbigint, 0) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

SELECT cfloat, cbigint, coalesce(cfloat, cbigint, 0) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT ctimestamp1, ctimestamp2, coalesce(ctimestamp1, ctimestamp2) as c
FROM alltypesorc
WHERE ctimestamp1 IS NOT NULL OR ctimestamp2 IS NOT NULL
ORDER BY ctimestamp1, ctimestamp2, c
LIMIT 10;

SELECT ctimestamp1, ctimestamp2, coalesce(ctimestamp1, ctimestamp2) as c
FROM alltypesorc
WHERE ctimestamp1 IS NOT NULL OR ctimestamp2 IS NOT NULL
ORDER BY ctimestamp1, ctimestamp2, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cfloat, cbigint, coalesce(cfloat, cbigint) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

SELECT cfloat, cbigint, coalesce(cfloat, cbigint) as c
FROM alltypesorc
WHERE (cfloat IS NULL AND cbigint IS NULL)
ORDER BY cfloat, cbigint, c
LIMIT 10;

EXPLAIN VECTORIZATION ONLY EXPRESSION SELECT cbigint, ctinyint, coalesce(cbigint, ctinyint) as c
FROM alltypesorc
WHERE cbigint IS NULL
LIMIT 10;

SELECT cbigint, ctinyint, coalesce(cbigint, ctinyint) as c
FROM alltypesorc
WHERE cbigint IS NULL
LIMIT 10;

CREATE TABLE test1 (
   col1 String,
   col2 decimal(18,6));

insert into test1 (col1, col2) values('hello', null);
select col1,col2 FROM test1
where (NVL(test1.col2, 0) = 0);

EXPLAIN VECTORIZATION DETAIL
select col1,col2 FROM test1
where (NVL(test1.col2, 0) = 0);
