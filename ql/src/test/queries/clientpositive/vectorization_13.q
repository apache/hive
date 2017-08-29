set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)) as c1,
         MAX(ctinyint) as c2,
         ((-(ctinyint)) + MAX(ctinyint)) as c3,
         SUM(cfloat) as c4,
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) as c5,
         (-(SUM(cfloat))) as c6,
         (79.553 * cfloat) as c7,
         STDDEV_POP(cfloat) as c8,
         (-(SUM(cfloat))) as c9,
         STDDEV_POP(ctinyint) as c10,
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175) as c11,
         (-((-(SUM(cfloat))))) as c12,
         (-26.28 / (-((-(SUM(cfloat)))))) as c13,
         MAX(cfloat) as c14,
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint) as c15,
         MIN(ctinyint) as c16
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > 11)
              AND ((ctimestamp2 != 12)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16
LIMIT 40;

SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)) as c1,
         MAX(ctinyint) as c2,
         ((-(ctinyint)) + MAX(ctinyint)) as c3,
         SUM(cfloat) as c4,
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) as c5,
         (-(SUM(cfloat))) as c6,
         (79.553 * cfloat) as c7,
         STDDEV_POP(cfloat) as c8,
         (-(SUM(cfloat))) as c9,
         STDDEV_POP(ctinyint) as c10,
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175) as c11,
         (-((-(SUM(cfloat))))) as c12,
         (-26.28 / (-((-(SUM(cfloat)))))) as c13,
         MAX(cfloat) as c14,
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint) as c15,
         MIN(ctinyint) as c16
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > 11)
              AND ((ctimestamp2 != 12)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16
LIMIT 40;

-- double compare timestamp
EXPLAIN VECTORIZATION EXPRESSION
SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)) as c1,
         MAX(ctinyint) as c2,
         ((-(ctinyint)) + MAX(ctinyint)) as c3,
         SUM(cfloat) as c4,
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) as c5,
         (-(SUM(cfloat))) as c6,
         (79.553 * cfloat) as c7,
         STDDEV_POP(cfloat) as c8,
         (-(SUM(cfloat))) as c9,
         STDDEV_POP(ctinyint) as c10,
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175) as c11,
         (-((-(SUM(cfloat))))) as c12,
         (-26.28 / (-((-(SUM(cfloat)))))) as c13,
         MAX(cfloat) as c14,
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint) as c15,
         MIN(ctinyint) as c16
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > -1.388)
              AND ((ctimestamp2 != -1.3359999999999999)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16
LIMIT 40;

SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)) as c1,
         MAX(ctinyint) as c2,
         ((-(ctinyint)) + MAX(ctinyint)) as c3,
         SUM(cfloat) as c4,
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) as c5,
         (-(SUM(cfloat))) as c6,
         (79.553 * cfloat) as c7,
         STDDEV_POP(cfloat) as c8,
         (-(SUM(cfloat))) as c9,
         STDDEV_POP(ctinyint) as c10,
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175) as c11,
         (-((-(SUM(cfloat))))) as c12,
         (-26.28 / (-((-(SUM(cfloat)))))) as c13,
         MAX(cfloat) as c14,
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint) as c15,
         MIN(ctinyint) as c16
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > -1.388)
              AND ((ctimestamp2 != -1.3359999999999999)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16
LIMIT 40;