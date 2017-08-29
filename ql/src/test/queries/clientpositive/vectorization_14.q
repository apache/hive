set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION DETAIL
SELECT   ctimestamp1,
         cfloat,
         cstring1,
         cboolean1,
         cdouble,
         (-26.28 + cdouble),
         (-((-26.28 + cdouble))),
         STDDEV_SAMP((-((-26.28 + cdouble)))),
         (cfloat * -26.28),
         MAX(cfloat),
         (-(cfloat)),
         (-(MAX(cfloat))),
         ((-((-26.28 + cdouble))) / 10.175),
         STDDEV_POP(cfloat),
         COUNT(cfloat),
         (-(((-((-26.28 + cdouble))) / 10.175))),
         (-1.389 % STDDEV_SAMP((-((-26.28 + cdouble))))),
         (cfloat - cdouble),
         VAR_POP(cfloat),
         (VAR_POP(cfloat) % 10.175),
         VAR_SAMP(cfloat),
         (-((cfloat - cdouble)))
FROM     alltypesorc
WHERE    (((ctinyint <= cbigint)
           AND ((cint <= cdouble)
                OR (ctimestamp2 < ctimestamp1)))
          AND ((cdouble < ctinyint)
              AND ((cbigint > -257)
                  OR (cfloat < cint))))
GROUP BY ctimestamp1, cfloat, cstring1, cboolean1, cdouble
ORDER BY cstring1, cfloat, cdouble, ctimestamp1;

SELECT   ctimestamp1,
         cfloat,
         cstring1,
         cboolean1,
         cdouble,
         (-26.28 + cdouble),
         (-((-26.28 + cdouble))),
         STDDEV_SAMP((-((-26.28 + cdouble)))),
         (cfloat * -26.28),
         MAX(cfloat),
         (-(cfloat)),
         (-(MAX(cfloat))),
         ((-((-26.28 + cdouble))) / 10.175),
         STDDEV_POP(cfloat),
         COUNT(cfloat),
         (-(((-((-26.28 + cdouble))) / 10.175))),
         (-1.389 % STDDEV_SAMP((-((-26.28 + cdouble))))),
         (cfloat - cdouble),
         VAR_POP(cfloat),
         (VAR_POP(cfloat) % 10.175),
         VAR_SAMP(cfloat),
         (-((cfloat - cdouble)))
FROM     alltypesorc
WHERE    (((ctinyint <= cbigint)
           AND ((cint <= cdouble)
                OR (ctimestamp2 < ctimestamp1)))
          AND ((cdouble < ctinyint)
              AND ((cbigint > -257)
                  OR (cfloat < cint))))
GROUP BY ctimestamp1, cfloat, cstring1, cboolean1, cdouble
ORDER BY cstring1, cfloat, cdouble, ctimestamp1;

