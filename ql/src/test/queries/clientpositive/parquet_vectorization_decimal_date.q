set hive.explain.user=false;
set hive.fetch.task.conversion=none;

CREATE TABLE date_decimal_test_parquet STORED AS PARQUET AS SELECT cint, cdouble, CAST (CAST (cint AS TIMESTAMP) AS DATE) AS cdate, CAST (((cdouble*22.1)/37) AS DECIMAL(20,10)) AS cdecimal FROM alltypesparquet;
SET hive.vectorized.execution.enabled=true;
EXPLAIN VECTORIZATION EXPRESSION  SELECT cdate, cdecimal from date_decimal_test_parquet where cint IS NOT NULL AND cdouble IS NOT NULL LIMIT 10;
SELECT cdate, cdecimal from date_decimal_test_parquet where cint IS NOT NULL AND cdouble IS NOT NULL LIMIT 10;
