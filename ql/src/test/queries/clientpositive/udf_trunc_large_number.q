-- SORT_QUERY_RESULTS
-- HIVE-26731: trunc must stay exact when the input, scaled to the requested
-- number of decimal places, no longer fits in a long.

SELECT trunc(123456711889.122345897, 8), trunc(-123456711889.122345897, 8),
       trunc(123456711889.122345897, 19), trunc(123456711889.122345897),
       trunc(123456711889.122345897, -4);

CREATE TABLE trunc_large_number (d DECIMAL(38,9), c DOUBLE, b BIGINT) STORED AS ORC;

INSERT INTO trunc_large_number VALUES
  (123456711889.122345897, 123456711889.12234, 9223372036854775807),
  (-123456711889.122345897, -123456711889.12234, -9223372036854775807),
  (12345678901234567890123456789.987654321, 0.3, 12345),
  (NULL, NULL, NULL);

set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;

EXPLAIN VECTORIZATION ONLY EXPRESSION
SELECT trunc(d, 8), trunc(d), trunc(c, 8), trunc(c) FROM trunc_large_number;

SELECT d, trunc(d, 8), trunc(d, -4), trunc(d), trunc(d, -30) FROM trunc_large_number;
SELECT c, trunc(c, 8), trunc(c, 1), trunc(c), trunc(c, -5) FROM trunc_large_number;
SELECT b, trunc(b, -3), trunc(b, -19) FROM trunc_large_number;

-- Row mode must give the same results as vectorized execution
set hive.vectorized.execution.enabled=false;

SELECT d, trunc(d, 8), trunc(d, -4), trunc(d), trunc(d, -30) FROM trunc_large_number;
SELECT c, trunc(c, 8), trunc(c, 1), trunc(c), trunc(c, -5) FROM trunc_large_number;
SELECT b, trunc(b, -3), trunc(b, -19) FROM trunc_large_number;

DROP TABLE trunc_large_number;
