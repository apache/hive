set hive.explain.user=false;
CREATE TABLE decimal_test STORED AS ORC AS SELECT cint, cdouble, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal1, CAST (((cdouble*9.3)/13) AS DECIMAL(23,14)) AS cdecimal2 FROM alltypesorc
WHERE cint is not null and cdouble is not null;

SET hive.vectorized.execution.enabled=true;

EXPLAIN
SELECT cint, cdouble, cdecimal1, cdecimal2, min(cdecimal1) as min_decimal1 FROM decimal_test
WHERE cdecimal1 is not null and cdecimal2 is not null
GROUP BY cint, cdouble, cdecimal1, cdecimal2
ORDER BY cint, cdouble, cdecimal1, cdecimal2
LIMIT 50;

SELECT cint, cdouble, cdecimal1, cdecimal2, min(cdecimal1) as min_decimal1 FROM decimal_test
WHERE cdecimal1 is not null and cdecimal2 is not null
GROUP BY cint, cdouble, cdecimal1, cdecimal2
ORDER BY cint, cdouble, cdecimal1, cdecimal2
LIMIT 50;

SET hive.vectorized.execution.enabled=false;

SELECT sum(hash(*))
  FROM (SELECT cint, cdouble, cdecimal1, cdecimal2, min(cdecimal1) as min_decimal1 FROM decimal_test
        WHERE cdecimal1 is not null and cdecimal2 is not null
        GROUP BY cint, cdouble, cdecimal1, cdecimal2
        ORDER BY cint, cdouble, cdecimal1, cdecimal2
        LIMIT 50) as q;

SET hive.vectorized.execution.enabled=true;

SELECT sum(hash(*))
  FROM (SELECT cint, cdouble, cdecimal1, cdecimal2, min(cdecimal1) as min_decimal1 FROM decimal_test
        WHERE cdecimal1 is not null and cdecimal2 is not null
        GROUP BY cint, cdouble, cdecimal1, cdecimal2
        ORDER BY cint, cdouble, cdecimal1, cdecimal2
        LIMIT 50) as q;
