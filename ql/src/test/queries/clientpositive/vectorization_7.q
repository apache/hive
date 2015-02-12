SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

EXPLAIN
SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint),
       (csmallint % -257),
       (-(csmallint)),
       (-(ctinyint)),
       ((-(ctinyint)) + 17),
       (cbigint * (-(csmallint))),
       (cint % csmallint),
       (-(ctinyint)),
       ((-(ctinyint)) % ctinyint)
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > -15)
                  AND (3569 >= cdouble)))))
LIMIT 25;

SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint),
       (csmallint % -257),
       (-(csmallint)),
       (-(ctinyint)),
       ((-(ctinyint)) + 17),
       (cbigint * (-(csmallint))),
       (cint % csmallint),
       (-(ctinyint)),
       ((-(ctinyint)) % ctinyint)
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > -15)
                  AND (3569 >= cdouble)))))
LIMIT 25;

-- double compare timestamp
EXPLAIN
SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint),
       (csmallint % -257),
       (-(csmallint)),
       (-(ctinyint)),
       ((-(ctinyint)) + 17),
       (cbigint * (-(csmallint))),
       (cint % csmallint),
       (-(ctinyint)),
       ((-(ctinyint)) % ctinyint)
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0.0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > 7.6850000000000005)
                  AND (3569 >= cdouble)))))
LIMIT 25;

SELECT cboolean1,
       cbigint,
       csmallint,
       ctinyint,
       ctimestamp1,
       cstring1,
       (cbigint + cbigint),
       (csmallint % -257),
       (-(csmallint)),
       (-(ctinyint)),
       ((-(ctinyint)) + 17),
       (cbigint * (-(csmallint))),
       (cint % csmallint),
       (-(ctinyint)),
       ((-(ctinyint)) % ctinyint)
FROM   alltypesorc
WHERE  ((ctinyint != 0)
        AND (((ctimestamp1 <= 0.0)
          OR ((ctinyint = cint)
               OR (cstring2 LIKE 'ss')))
          AND ((988888 < cdouble)
              OR ((ctimestamp2 > 7.6850000000000005)
                  AND (3569 >= cdouble)))))
LIMIT 25;

