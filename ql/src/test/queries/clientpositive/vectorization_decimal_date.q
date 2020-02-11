--! qt:dataset:alltypesorc
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

CREATE TABLE date_decimal_test STORED AS ORC AS SELECT cint, cdouble, CAST (CAST (cint AS TIMESTAMP) AS DATE) AS cdate, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal FROM alltypesorc;
SET hive.vectorized.execution.enabled=true;
EXPLAIN VECTORIZATION EXPRESSION  SELECT cdate, cint, cdecimal from date_decimal_test where cint IS NOT NULL AND cdouble IS NOT NULL LIMIT 10;
-- 528534767 is 'Wednesday, January 7, 1970 2:48:54 AM'
SELECT cdate, cint, cdecimal from date_decimal_test where cint IS NOT NULL AND cdouble IS NOT NULL LIMIT 10;
