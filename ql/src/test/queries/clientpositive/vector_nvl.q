SET hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;

EXPLAIN SELECT cdouble, nvl(cdouble, 100) as n
FROM alltypesorc
WHERE (cdouble IS NULL)
LIMIT 10;

SELECT cdouble, nvl(cdouble, 100) as n
FROM alltypesorc
WHERE (cdouble IS NULL)
LIMIT 10;

EXPLAIN SELECT cfloat, nvl(cfloat, 1) as n
FROM alltypesorc
LIMIT 10;

SELECT cfloat, nvl(cfloat, 1) as n
FROM alltypesorc
LIMIT 10;

EXPLAIN SELECT nvl(null, 10) as n
FROM alltypesorc
LIMIT 10;

SELECT nvl(null, 10) as n
FROM alltypesorc
LIMIT 10;

EXPLAIN SELECT nvl(null, null) as n
FROM alltypesorc
LIMIT 10;

SELECT nvl(null, null) as n
FROM alltypesorc
LIMIT 10;
