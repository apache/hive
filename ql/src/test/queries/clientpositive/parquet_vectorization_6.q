SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT cboolean1,
       cfloat,
       cstring1,
       (988888 * csmallint),
       (-(csmallint)),
       (-(cfloat)),
       (-26.28 / cfloat),
       (cfloat * 359),
       (cint % ctinyint),
       (-(cdouble)),
       (ctinyint - -75),
       (762 * (cint % ctinyint))
FROM   alltypesparquet
WHERE  ((ctinyint != 0)
        AND ((((cboolean1 <= 0)
          AND (cboolean2 >= cboolean1))
          OR ((cbigint IS NOT NULL)
              AND ((cstring2 LIKE '%a')
                   OR (cfloat <= -257))))));

SELECT cboolean1,
       cfloat,
       cstring1,
       (988888 * csmallint),
       (-(csmallint)),
       (-(cfloat)),
       (-26.28 / cfloat),
       (cfloat * 359),
       (cint % ctinyint),
       (-(cdouble)),
       (ctinyint - -75),
       (762 * (cint % ctinyint))
FROM   alltypesparquet
WHERE  ((ctinyint != 0)
        AND ((((cboolean1 <= 0)
          AND (cboolean2 >= cboolean1))
          OR ((cbigint IS NOT NULL)
              AND ((cstring2 LIKE '%a')
                   OR (cfloat <= -257))))));

