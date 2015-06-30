set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

-- SORT_QUERY_RESULTS

EXPLAIN
SELECT ctimestamp1,
       cdouble,
       cboolean1,
       cstring1,
       cfloat,
       (-(cdouble)) as c1,
       (-5638.15 - cdouble) as c2,
       (cdouble * -257) as c3,
       (cint + cfloat) as c4,
       ((-(cdouble)) + cbigint) as c5,
       (-(cdouble)) as c6,
       (-1.389 - cfloat) as c7,
       (-(cfloat)) as c8,
       ((-5638.15 - cdouble) + (cint + cfloat)) as c9
FROM   alltypesorc
WHERE  (((cstring2 IS NOT NULL)
         AND ((ctimestamp1 <= 10)
             AND (ctimestamp2 != 16)))
        OR ((cfloat < -6432)
            OR ((cboolean1 IS NOT NULL)
                AND (cdouble = 988888))))
ORDER BY ctimestamp1, cdouble, cboolean1, cstring1, cfloat, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 20;

SELECT ctimestamp1,
       cdouble,
       cboolean1,
       cstring1,
       cfloat,
       (-(cdouble)) as c1,
       (-5638.15 - cdouble) as c2,
       (cdouble * -257) as c3,
       (cint + cfloat) as c4,
       ((-(cdouble)) + cbigint) as c5,
       (-(cdouble)) as c6,
       (-1.389 - cfloat) as c7,
       (-(cfloat)) as c8,
       ((-5638.15 - cdouble) + (cint + cfloat)) as c9
FROM   alltypesorc
WHERE  (((cstring2 IS NOT NULL)
         AND ((ctimestamp1 <= 10)
             AND (ctimestamp2 != 16)))
        OR ((cfloat < -6432)
            OR ((cboolean1 IS NOT NULL)
                AND (cdouble = 988888))))
ORDER BY ctimestamp1, cdouble, cboolean1, cstring1, cfloat, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 20;


-- double compare timestamp
EXPLAIN
SELECT ctimestamp1,
       cdouble,
       cboolean1,
       cstring1,
       cfloat,
       (-(cdouble)) as c1,
       (-5638.15 - cdouble) as c2,
       (cdouble * -257) as c3,
       (cint + cfloat) as c4,
       ((-(cdouble)) + cbigint) as c5,
       (-(cdouble)) as c6,
       (-1.389 - cfloat) as c7,
       (-(cfloat)) as c8,
       ((-5638.15 - cdouble) + (cint + cfloat)) as c9
FROM   alltypesorc
WHERE  (((cstring2 IS NOT NULL)
         AND ((ctimestamp1 <= 12.503)
             AND (ctimestamp2 != 11.998)))
        OR ((cfloat < -6432)
            OR ((cboolean1 IS NOT NULL)
                AND (cdouble = 988888))))
ORDER BY ctimestamp1, cdouble, cboolean1, cstring1, cfloat, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 20;

SELECT ctimestamp1,
       cdouble,
       cboolean1,
       cstring1,
       cfloat,
       (-(cdouble)) as c1,
       (-5638.15 - cdouble) as c2,
       (cdouble * -257) as c3,
       (cint + cfloat) as c4,
       ((-(cdouble)) + cbigint) as c5,
       (-(cdouble)) as c6,
       (-1.389 - cfloat) as c7,
       (-(cfloat)) as c8,
       ((-5638.15 - cdouble) + (cint + cfloat)) as c9
FROM   alltypesorc
WHERE  (((cstring2 IS NOT NULL)
         AND ((ctimestamp1 <= 12.503)
             AND (ctimestamp2 != 11.998)))
        OR ((cfloat < -6432)
            OR ((cboolean1 IS NOT NULL)
                AND (cdouble = 988888))))
ORDER BY ctimestamp1, cdouble, cboolean1, cstring1, cfloat, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 20;

