set hive.explain.user=false;
set hive.fetch.task.conversion=none;

CREATE TABLE decimal_vgby STORED AS ORC AS 
    SELECT cdouble, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal1, 
    CAST (((cdouble*9.3)/13) AS DECIMAL(23,14)) AS cdecimal2,
    cint
    FROM alltypesorc;

SET hive.vectorized.execution.enabled=true;

-- SORT_QUERY_RESULTS

-- First only do simple aggregations that output primitives only
EXPLAIN VECTORIZATION EXPRESSION SELECT cint,
    COUNT(cdecimal1), MAX(cdecimal1), MIN(cdecimal1), SUM(cdecimal1),
    COUNT(cdecimal2), MAX(cdecimal2), MIN(cdecimal2), SUM(cdecimal2)
    FROM decimal_vgby
    GROUP BY cint
    HAVING COUNT(*) > 1;
SELECT cint,
    COUNT(cdecimal1), MAX(cdecimal1), MIN(cdecimal1), SUM(cdecimal1),
    COUNT(cdecimal2), MAX(cdecimal2), MIN(cdecimal2), SUM(cdecimal2)
    FROM decimal_vgby
    GROUP BY cint
    HAVING COUNT(*) > 1;

-- Now add the others...
EXPLAIN VECTORIZATION EXPRESSION SELECT cint,
    COUNT(cdecimal1), MAX(cdecimal1), MIN(cdecimal1), SUM(cdecimal1), AVG(cdecimal1), STDDEV_POP(cdecimal1), STDDEV_SAMP(cdecimal1),
    COUNT(cdecimal2), MAX(cdecimal2), MIN(cdecimal2), SUM(cdecimal2), AVG(cdecimal2), STDDEV_POP(cdecimal2), STDDEV_SAMP(cdecimal2)
    FROM decimal_vgby
    GROUP BY cint
    HAVING COUNT(*) > 1;
SELECT cint,
    COUNT(cdecimal1), MAX(cdecimal1), MIN(cdecimal1), SUM(cdecimal1), AVG(cdecimal1), STDDEV_POP(cdecimal1), STDDEV_SAMP(cdecimal1),
    COUNT(cdecimal2), MAX(cdecimal2), MIN(cdecimal2), SUM(cdecimal2), AVG(cdecimal2), STDDEV_POP(cdecimal2), STDDEV_SAMP(cdecimal2)
    FROM decimal_vgby
    GROUP BY cint
    HAVING COUNT(*) > 1;