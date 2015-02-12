SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

EXPLAIN
SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)),
         MAX(ctinyint),
         ((-(ctinyint)) + MAX(ctinyint)),
         SUM(cfloat),
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))),
         (-(SUM(cfloat))),
         (79.553 * cfloat),
         STDDEV_POP(cfloat),
         (-(SUM(cfloat))),
         STDDEV_POP(ctinyint),
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175),
         (-((-(SUM(cfloat))))),
         (-26.28 / (-((-(SUM(cfloat)))))),
         MAX(cfloat),
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint),
         MIN(ctinyint)
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > 11)
              AND ((ctimestamp2 != 12)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
LIMIT 40;

SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)),
         MAX(ctinyint),
         ((-(ctinyint)) + MAX(ctinyint)),
         SUM(cfloat),
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))),
         (-(SUM(cfloat))),
         (79.553 * cfloat),
         STDDEV_POP(cfloat),
         (-(SUM(cfloat))),
         STDDEV_POP(ctinyint),
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175),
         (-((-(SUM(cfloat))))),
         (-26.28 / (-((-(SUM(cfloat)))))),
         MAX(cfloat),
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint),
         MIN(ctinyint)
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > 11)
              AND ((ctimestamp2 != 12)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
LIMIT 40;

-- double compare timestamp
EXPLAIN
SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)),
         MAX(ctinyint),
         ((-(ctinyint)) + MAX(ctinyint)),
         SUM(cfloat),
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))),
         (-(SUM(cfloat))),
         (79.553 * cfloat),
         STDDEV_POP(cfloat),
         (-(SUM(cfloat))),
         STDDEV_POP(ctinyint),
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175),
         (-((-(SUM(cfloat))))),
         (-26.28 / (-((-(SUM(cfloat)))))),
         MAX(cfloat),
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint),
         MIN(ctinyint)
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > -1.388)
              AND ((ctimestamp2 != -1.3359999999999999)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
LIMIT 40;

SELECT   cboolean1,
         ctinyint,
         ctimestamp1,
         cfloat,
         cstring1,
         (-(ctinyint)),
         MAX(ctinyint),
         ((-(ctinyint)) + MAX(ctinyint)),
         SUM(cfloat),
         (SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))),
         (-(SUM(cfloat))),
         (79.553 * cfloat),
         STDDEV_POP(cfloat),
         (-(SUM(cfloat))),
         STDDEV_POP(ctinyint),
         (((-(ctinyint)) + MAX(ctinyint)) - 10.175),
         (-((-(SUM(cfloat))))),
         (-26.28 / (-((-(SUM(cfloat)))))),
         MAX(cfloat),
         ((SUM(cfloat) * ((-(ctinyint)) + MAX(ctinyint))) / ctinyint),
         MIN(ctinyint)
FROM     alltypesorc
WHERE    (((cfloat < 3569)
           AND ((10.175 >= cdouble)
                AND (cboolean1 != 1)))
          OR ((ctimestamp1 > -1.388)
              AND ((ctimestamp2 != -1.3359999999999999)
                   AND (ctinyint < 9763215.5639))))
GROUP BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
ORDER BY cboolean1, ctinyint, ctimestamp1, cfloat, cstring1
LIMIT 40;

