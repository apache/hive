set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

-- SORT_QUERY_RESULTS

EXPLAIN
SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint) as c1,
       (csmallint % -257) as c2,
       (-(csmallint)) as c3,
       (-(ctinyint)) as c4,
       ((-(ctinyint)) + 17) as c5,
       (cbigint * (-(csmallint))) as c6,
       (cint % csmallint) as c7,
       (-(ctinyint)) as c8,
       ((-(ctinyint)) % ctinyint) as c9
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > -15)
                  AND (3569 >= cdouble)))))
ORDER BY cboolean1, cbigint, csmallint, ctinyint, ctimestamp1, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 25;

SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint) as c1,
       (csmallint % -257) as c2,
       (-(csmallint)) as c3,
       (-(ctinyint)) as c4,
       ((-(ctinyint)) + 17) as c5,
       (cbigint * (-(csmallint))) as c6,
       (cint % csmallint) as c7,
       (-(ctinyint)) as c8,
       ((-(ctinyint)) % ctinyint) as c9
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > -15)
                  AND (3569 >= cdouble)))))
ORDER BY cboolean1, cbigint, csmallint, ctinyint, ctimestamp1, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 25;


-- double compare timestamp
EXPLAIN
SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint) as c1,
       (csmallint % -257) as c2,
       (-(csmallint)) as c3,
       (-(ctinyint)) as c4,
       ((-(ctinyint)) + 17) as c5,
       (cbigint * (-(csmallint))) as c6,
       (cint % csmallint) as c7,
       (-(ctinyint)) as c8,
       ((-(ctinyint)) % ctinyint) as c9
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0.0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > 7.6850000000000005)
                  AND (3569 >= cdouble)))))
ORDER BY cboolean1, cbigint, csmallint, ctinyint, ctimestamp1, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 25;

SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint) as c1,
       (csmallint % -257) as c2,
       (-(csmallint)) as c3,
       (-(ctinyint)) as c4,
       ((-(ctinyint)) + 17) as c5,
       (cbigint * (-(csmallint))) as c6,
       (cint % csmallint) as c7,
       (-(ctinyint)) as c8,
       ((-(ctinyint)) % ctinyint) as c9
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0.0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > 7.6850000000000005)
                  AND (3569 >= cdouble)))))
ORDER BY cboolean1, cbigint, csmallint, ctinyint, ctimestamp1, cstring1, c1, c2, c3, c4, c5, c6, c7, c8, c9
LIMIT 25;

