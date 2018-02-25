set hive.vectorized.execution.enabled=true;


EXPLAIN VECTORIZATION DETAIL
SELECT AVG(cint),
       (AVG(cint) + -3728),
       (-((AVG(cint) + -3728))),
       (-((-((AVG(cint) + -3728))))),
       ((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)),
       SUM(cdouble),
       (-(AVG(cint))),
       STDDEV_POP(cint),
       (((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)) * (-((-((AVG(cint) + -3728)))))),
       STDDEV_SAMP(csmallint),
       (-(STDDEV_POP(cint))),
       (STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))),
       ((STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))) * STDDEV_POP(cint)),
       VAR_SAMP(cint),
       AVG(cfloat),
       (10.175 - VAR_SAMP(cint)),
       (-((10.175 - VAR_SAMP(cint)))),
       ((-(STDDEV_POP(cint))) / -563),
       STDDEV_SAMP(cint),
       (-(((-(STDDEV_POP(cint))) / -563))),
       (AVG(cint) / SUM(cdouble)),
       MIN(ctinyint),
       COUNT(csmallint),
       (MIN(ctinyint) / ((-(STDDEV_POP(cint))) / -563)),
       (-((AVG(cint) / SUM(cdouble))))
FROM   alltypesorc
WHERE  ((762 = cbigint)
        OR ((csmallint < cfloat)
            AND ((ctimestamp2 > -5)
                 AND (cdouble != cint)))
        OR (cstring1 = 'a')
           OR ((cbigint <= -1.389)
               AND ((cstring2 != 'a')
                    AND ((79.553 != cint)
                         AND (cboolean2 != cboolean1)))));

set hive.vectorized.reuse.scratch.columns=false;

EXPLAIN VECTORIZATION DETAIL
SELECT AVG(cint),
       (AVG(cint) + -3728),
       (-((AVG(cint) + -3728))),
       (-((-((AVG(cint) + -3728))))),
       ((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)),
       SUM(cdouble),
       (-(AVG(cint))),
       STDDEV_POP(cint),
       (((-((-((AVG(cint) + -3728))))) * (AVG(cint) + -3728)) * (-((-((AVG(cint) + -3728)))))),
       STDDEV_SAMP(csmallint),
       (-(STDDEV_POP(cint))),
       (STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))),
       ((STDDEV_POP(cint) - (-((-((AVG(cint) + -3728)))))) * STDDEV_POP(cint)),
       VAR_SAMP(cint),
       AVG(cfloat),
       (10.175 - VAR_SAMP(cint)),
       (-((10.175 - VAR_SAMP(cint)))),
       ((-(STDDEV_POP(cint))) / -563),
       STDDEV_SAMP(cint),
       (-(((-(STDDEV_POP(cint))) / -563))),
       (AVG(cint) / SUM(cdouble)),
       MIN(ctinyint),
       COUNT(csmallint),
       (MIN(ctinyint) / ((-(STDDEV_POP(cint))) / -563)),
       (-((AVG(cint) / SUM(cdouble))))
FROM   alltypesorc
WHERE  ((762 = cbigint)
        OR ((csmallint < cfloat)
            AND ((ctimestamp2 > -5)
                 AND (cdouble != cint)))
        OR (cstring1 = 'a')
           OR ((cbigint <= -1.389)
               AND ((cstring2 != 'a')
                    AND ((79.553 != cint)
                         AND (cboolean2 != cboolean1)))));

