set hive.explain.user=false;

-- SORT_QUERY_RESULTS

CREATE TABLE decimal_test STORED AS ORC AS SELECT cdouble, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal1, CAST (((cdouble*9.3)/13) AS DECIMAL(23,14)) AS cdecimal2 FROM alltypesorc;
SET hive.vectorized.execution.enabled=true;
EXPLAIN SELECT cdecimal1 + cdecimal2 as c1, cdecimal1 - (2*cdecimal2) as c2, ((cdecimal1+2.34)/cdecimal2) as c3, (cdecimal1 * (cdecimal2/3.4)) as c4, cdecimal1 % 10 as c5, CAST(cdecimal1 AS INT) as c6, CAST(cdecimal2 AS SMALLINT) as c7, CAST(cdecimal2 AS TINYINT) as c8, CAST(cdecimal1 AS BIGINT) as c9, CAST (cdecimal1 AS BOOLEAN) as c10, CAST(cdecimal2 AS DOUBLE) as c11, CAST(cdecimal1 AS FLOAT) as c12, CAST(cdecimal2 AS STRING) as c13, CAST(cdecimal1 AS TIMESTAMP) as c14 FROM decimal_test WHERE cdecimal1 > 0 AND cdecimal1 < 12345.5678 AND cdecimal2 != 0 AND cdecimal2 > 1000 AND cdouble IS NOT NULL
ORDER BY c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14
LIMIT 10;

SELECT cdecimal1 + cdecimal2 as c1, cdecimal1 - (2*cdecimal2) as c2, ((cdecimal1+2.34)/cdecimal2) as c3, (cdecimal1 * (cdecimal2/3.4)) as c4, cdecimal1 % 10 as c5, CAST(cdecimal1 AS INT) as c6, CAST(cdecimal2 AS SMALLINT) as c7, CAST(cdecimal2 AS TINYINT) as c8, CAST(cdecimal1 AS BIGINT) as c9, CAST (cdecimal1 AS BOOLEAN) as c10, CAST(cdecimal2 AS DOUBLE) as c11, CAST(cdecimal1 AS FLOAT) as c12, CAST(cdecimal2 AS STRING) as c13, CAST(cdecimal1 AS TIMESTAMP) as c14 FROM decimal_test WHERE cdecimal1 > 0 AND cdecimal1 < 12345.5678 AND cdecimal2 != 0 AND cdecimal2 > 1000 AND cdouble IS NOT NULL
ORDER BY c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14
LIMIT 10;