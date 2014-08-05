SET hive.vectorized.execution.enabled=true;

EXPLAIN 
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

SELECT   cfloat,
         cstring1,
         cint,
         ctimestamp1,
         cdouble,
         cbigint,
         (cfloat / ctinyint),
         (cint % cbigint),
         (-(cdouble)),
         (cdouble + (cfloat / ctinyint)),
         (cdouble / cint),
         (-((-(cdouble)))),
         (9763215.5639 % cbigint),
         (2563.58 + (-((-(cdouble)))))
FROM     alltypesorc
WHERE    (((cbigint > -23)
           AND ((cdouble != 988888)
                OR (cint > -863.257)))
          AND ((ctinyint >= 33)
              OR ((csmallint >= cbigint)
                  OR (cfloat = cdouble))))
ORDER BY cbigint, cfloat;

