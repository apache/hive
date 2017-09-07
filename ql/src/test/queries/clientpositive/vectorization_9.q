set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT   cstring1,
         cdouble,
         ctimestamp1,
         (cdouble - 9763215.5639),
         (-((cdouble - 9763215.5639))),
         COUNT(cdouble),
         STDDEV_SAMP(cdouble),
         (-(STDDEV_SAMP(cdouble))),
         (STDDEV_SAMP(cdouble) * COUNT(cdouble)),
         MIN(cdouble),
         (9763215.5639 / cdouble),
         (COUNT(cdouble) / -1.389),
         STDDEV_SAMP(cdouble)
FROM     alltypesorc
WHERE    ((cstring2 LIKE '%b%')
          AND ((cdouble >= -1.389)
              OR (cstring1 < 'a')))
GROUP BY cstring1, cdouble, ctimestamp1;

SELECT   cstring1,
         cdouble,
         ctimestamp1,
         (cdouble - 9763215.5639),
         (-((cdouble - 9763215.5639))),
         COUNT(cdouble),
         STDDEV_SAMP(cdouble),
         (-(STDDEV_SAMP(cdouble))),
         (STDDEV_SAMP(cdouble) * COUNT(cdouble)),
         MIN(cdouble),
         (9763215.5639 / cdouble),
         (COUNT(cdouble) / -1.389),
         STDDEV_SAMP(cdouble)
FROM     alltypesorc
WHERE    ((cstring2 LIKE '%b%')
          AND ((cdouble >= -1.389)
              OR (cstring1 < 'a')))
GROUP BY cstring1, cdouble, ctimestamp1;

