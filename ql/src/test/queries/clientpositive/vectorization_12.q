set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT   cbigint,
         cboolean1,
         cstring1,
         ctimestamp1,
         cdouble,
         (-6432 * cdouble),
         (-(cbigint)),
         COUNT(cbigint),
         (cbigint * COUNT(cbigint)),
         STDDEV_SAMP(cbigint),
         ((-6432 * cdouble) / -6432),
         (-(((-6432 * cdouble) / -6432))),
         AVG(cdouble),
         (-((-6432 * cdouble))),
         (-5638.15 + cbigint),
         SUM(cbigint),
         (AVG(cdouble) / (-6432 * cdouble)),
         AVG(cdouble),
         (-((-(((-6432 * cdouble) / -6432))))),
         (((-6432 * cdouble) / -6432) + (-((-6432 * cdouble)))),
         STDDEV_POP(cdouble)
FROM     alltypesorc
WHERE    (((ctimestamp1 IS NULL)
           AND ((cboolean1 >= cboolean2)
                OR (ctinyint != csmallint)))
          AND ((cstring1 LIKE '%a')
              OR ((cboolean2 <= 1)
                  AND (cbigint >= csmallint))))
GROUP BY cbigint, cboolean1, cstring1, ctimestamp1, cdouble
ORDER BY ctimestamp1, cdouble, cbigint, cstring1;

SELECT   cbigint,
         cboolean1,
         cstring1,
         ctimestamp1,
         cdouble,
         (-6432 * cdouble),
         (-(cbigint)),
         COUNT(cbigint),
         (cbigint * COUNT(cbigint)),
         STDDEV_SAMP(cbigint),
         ((-6432 * cdouble) / -6432),
         (-(((-6432 * cdouble) / -6432))),
         AVG(cdouble),
         (-((-6432 * cdouble))),
         (-5638.15 + cbigint),
         SUM(cbigint),
         (AVG(cdouble) / (-6432 * cdouble)),
         AVG(cdouble),
         (-((-(((-6432 * cdouble) / -6432))))),
         (((-6432 * cdouble) / -6432) + (-((-6432 * cdouble)))),
         STDDEV_POP(cdouble)
FROM     alltypesorc
WHERE    (((ctimestamp1 IS NULL)
           AND ((cboolean1 >= cboolean2)
                OR (ctinyint != csmallint)))
          AND ((cstring1 LIKE '%a')
              OR ((cboolean2 <= 1)
                  AND (cbigint >= csmallint))))
GROUP BY cbigint, cboolean1, cstring1, ctimestamp1, cdouble
ORDER BY ctimestamp1, cdouble, cbigint, cstring1;

