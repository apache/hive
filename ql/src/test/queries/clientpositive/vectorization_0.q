SET hive.vectorized.execution.enabled=true;

-- Use ORDER BY clauses to generate 2 stages.
EXPLAIN
SELECT MIN(ctinyint) as c1,
       MAX(ctinyint),
       COUNT(ctinyint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

SELECT MIN(ctinyint) as c1,
       MAX(ctinyint),
       COUNT(ctinyint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

EXPLAIN
SELECT SUM(ctinyint) as c1
FROM   alltypesorc
ORDER BY c1;

SELECT SUM(ctinyint) as c1
FROM   alltypesorc
ORDER BY c1;

EXPLAIN 
SELECT
  avg(ctinyint) as c1,
  variance(ctinyint),
  var_pop(ctinyint),
  var_samp(ctinyint),
  std(ctinyint),
  stddev(ctinyint),
  stddev_pop(ctinyint),
  stddev_samp(ctinyint)
FROM alltypesorc
ORDER BY c1;

SELECT
  avg(ctinyint) as c1,
  variance(ctinyint),
  var_pop(ctinyint),
  var_samp(ctinyint),
  std(ctinyint),
  stddev(ctinyint),
  stddev_pop(ctinyint),
  stddev_samp(ctinyint)
FROM alltypesorc
ORDER BY c1;

EXPLAIN
SELECT MIN(cbigint) as c1,
       MAX(cbigint),
       COUNT(cbigint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

SELECT MIN(cbigint) as c1,
       MAX(cbigint),
       COUNT(cbigint),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

EXPLAIN
SELECT SUM(cbigint) as c1
FROM   alltypesorc
ORDER BY c1;

SELECT SUM(cbigint) as c1
FROM   alltypesorc
ORDER BY c1;

EXPLAIN 
SELECT
  avg(cbigint) as c1,
  variance(cbigint),
  var_pop(cbigint),
  var_samp(cbigint),
  std(cbigint),
  stddev(cbigint),
  stddev_pop(cbigint),
  stddev_samp(cbigint)
FROM alltypesorc
ORDER BY c1;

SELECT
  avg(cbigint) as c1,
  variance(cbigint),
  var_pop(cbigint),
  var_samp(cbigint),
  std(cbigint),
  stddev(cbigint),
  stddev_pop(cbigint),
  stddev_samp(cbigint)
FROM alltypesorc
ORDER BY c1;

EXPLAIN
SELECT MIN(cfloat) as c1,
       MAX(cfloat),
       COUNT(cfloat),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

SELECT MIN(cfloat) as c1,
       MAX(cfloat),
       COUNT(cfloat),
       COUNT(*)
FROM   alltypesorc
ORDER BY c1;

EXPLAIN
SELECT SUM(cfloat) as c1
FROM   alltypesorc
ORDER BY c1;

SELECT SUM(cfloat) as c1
FROM   alltypesorc
ORDER BY c1;

EXPLAIN 
SELECT
  avg(cfloat) as c1,
  variance(cfloat),
  var_pop(cfloat),
  var_samp(cfloat),
  std(cfloat),
  stddev(cfloat),
  stddev_pop(cfloat),
  stddev_samp(cfloat)
FROM alltypesorc
ORDER BY c1;

SELECT
  avg(cfloat) as c1,
  variance(cfloat),
  var_pop(cfloat),
  var_samp(cfloat),
  std(cfloat),
  stddev(cfloat),
  stddev_pop(cfloat),
  stddev_samp(cfloat)
FROM alltypesorc
ORDER BY c1;

EXPLAIN
SELECT AVG(cbigint),
       (-(AVG(cbigint))),
       (-6432 + AVG(cbigint)),
       STDDEV_POP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) + (-6432 + AVG(cbigint))),
       VAR_SAMP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       (-6432 + (-((-6432 + AVG(cbigint))))),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) / (-((-6432 + AVG(cbigint))))),
       COUNT(*),
       SUM(cfloat),
       (VAR_SAMP(cbigint) % STDDEV_POP(cbigint)),
       (-(VAR_SAMP(cbigint))),
       ((-((-6432 + AVG(cbigint)))) * (-(AVG(cbigint)))),
       MIN(ctinyint),
       (-(MIN(ctinyint)))
FROM   alltypesorc
WHERE  (((cstring2 LIKE '%b%')
         OR ((79.553 != cint)
             OR (cbigint < cdouble)))
        OR ((ctinyint >= csmallint)
            AND ((cboolean2 = 1)
                 AND (3569 = ctinyint))));

SELECT AVG(cbigint),
       (-(AVG(cbigint))),
       (-6432 + AVG(cbigint)),
       STDDEV_POP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) + (-6432 + AVG(cbigint))),
       VAR_SAMP(cbigint),
       (-((-6432 + AVG(cbigint)))),
       (-6432 + (-((-6432 + AVG(cbigint))))),
       (-((-6432 + AVG(cbigint)))),
       ((-((-6432 + AVG(cbigint)))) / (-((-6432 + AVG(cbigint))))),
       COUNT(*),
       SUM(cfloat),
       (VAR_SAMP(cbigint) % STDDEV_POP(cbigint)),
       (-(VAR_SAMP(cbigint))),
       ((-((-6432 + AVG(cbigint)))) * (-(AVG(cbigint)))),
       MIN(ctinyint),
       (-(MIN(ctinyint)))
FROM   alltypesorc
WHERE  (((cstring2 LIKE '%b%')
         OR ((79.553 != cint)
             OR (cbigint < cdouble)))
        OR ((ctinyint >= csmallint)
            AND ((cboolean2 = 1)
                 AND (3569 = ctinyint))));

