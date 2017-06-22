SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT cdouble,
       ctimestamp1,
       ctinyint,
       cboolean1,
       cstring1,
       (-(cdouble)),
       (cdouble + csmallint),
       ((cdouble + csmallint) % 33),
       (-(cdouble)),
       (ctinyint % cdouble),
       (ctinyint % csmallint),
       (-(cdouble)),
       (cbigint * (ctinyint % csmallint)),
       (9763215.5639 - (cdouble + csmallint)),
       (-((-(cdouble))))
FROM   alltypesorc
WHERE  (((cstring2 <= '10')
         OR ((ctinyint > cdouble)
             AND (-5638.15 >= ctinyint)))
        OR ((cdouble > 6981)
            AND ((csmallint = 9763215.5639)
                 OR (cstring1 LIKE '%a'))));

SELECT cdouble,
       ctimestamp1,
       ctinyint,
       cboolean1,
       cstring1,
       (-(cdouble)),
       (cdouble + csmallint),
       ((cdouble + csmallint) % 33),
       (-(cdouble)),
       (ctinyint % cdouble),
       (ctinyint % csmallint),
       (-(cdouble)),
       (cbigint * (ctinyint % csmallint)),
       (9763215.5639 - (cdouble + csmallint)),
       (-((-(cdouble))))
FROM   alltypesorc
WHERE  (((cstring2 <= '10')
         OR ((ctinyint > cdouble)
             AND (-5638.15 >= ctinyint)))
        OR ((cdouble > 6981)
            AND ((csmallint = 9763215.5639)
                 OR (cstring1 LIKE '%a'))));

