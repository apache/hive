SET hive.cli.print.header=true;

CREATE TABLE window_decimal_test (
  id INT,
  decimal_col DECIMAL(10,2)
);

INSERT INTO window_decimal_test VALUES
  (1, NULL),
  (2, NULL),
  (1, NULL),
  (1, NULL),
  (2, 200.00),
  (3, NULL),
  (6, 610.00),
  (2, NULL),
  (3, 300.75),
  (4, NULL),
  (3, 300.50),
  (5, 500.20),
  (4, 400.00),
  (5, NULL),
  (6, 600.00),
  (7, NULL),
  (NULL, 800.00),
  (NULL, NULL);

-- ================ Test FIRST_VALUE and LAST_VALUE for decimal column ====================================================

-- Test FIRST_VALUE and LAST_VALUE for decimal column with IGNORE NULLS outside function
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column with IGNORE NULLS inside function
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column with PARTITION BY clause
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column with PARTITION BY clause and without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id) AS last_decimal
FROM window_decimal_test;


--========================================================================================
-- Repeat the same tests with nulls first added after order by clause
--========================================================================================
-- Test FIRST_VALUE and LAST_VALUE for decimal column with IGNORE NULLS outside function
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column with IGNORE NULLS inside function
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column with PARTITION BY clause
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

-- Test FIRST_VALUE and LAST_VALUE for decimal column with PARTITION BY clause and without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

set hive.vectorized.execution.enabled=true;
SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;


--========================================================================================
-- Test with reducer batch size set to 2
--========================================================================================
set hive.vectorized.testing.reducer.batch.size=2;
set hive.vectorized.execution.enabled=true;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(ORDER BY id) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id) AS last_decimal
FROM window_decimal_test;

SELECT id, decimal_col,
  FIRST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_decimal,
  LAST_VALUE(decimal_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_decimal
FROM window_decimal_test;
