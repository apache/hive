SET hive.cli.print.header=true;

CREATE TABLE window_double_test (
  id INT,
  double_col DOUBLE
);

INSERT INTO window_double_test VALUES
  (1, NULL),
  (2, NULL),
  (1, NULL),
  (1, NULL),
  (2, 25.5),
  (3, NULL),
  (6, 65.5),
  (2, NULL),
  (3, 32.5),
  (4, NULL),
  (3, 30.5),
  (5, 50.5),
  (4, 42.3),
  (5, NULL),
  (6, 65.2),
  (7, NULL),
  (NULL, 80.5),
  (NULL, NULL);
  
-- ================ Test FIRST_VALUE and LAST_VALUE for double column ====================================================

-- Test FIRST_VALUE and LAST_VALUE for double column with IGNORE NULLS outside function
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column with IGNORE NULLS inside function
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column with PARTITION BY clause
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col) OVER(ORDER BY id) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col) OVER(ORDER BY id) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column with PARTITION BY clause and without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id) AS first_double,
  LAST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id) AS first_double,
  LAST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id) AS last_double
FROM window_double_test;


--========================================================================================
-- Repeat the same tests with nulls first added after order by clause
--========================================================================================
-- Test FIRST_VALUE and LAST_VALUE for double column with IGNORE NULLS outside function
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column with IGNORE NULLS inside function
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column with PARTITION BY clause
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

-- Test FIRST_VALUE and LAST_VALUE for double column with PARTITION BY clause and without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

set hive.vectorized.execution.enabled=true;
SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;


--========================================================================================
-- Test with reducer batch size set to 2
--========================================================================================
set hive.vectorized.testing.reducer.batch.size=2;
set hive.vectorized.execution.enabled=true;

SELECT id, double_col,
  FIRST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(ORDER BY id) AS first_double,
  LAST_VALUE(double_col) OVER(ORDER BY id) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id) AS first_double,
  LAST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id) AS last_double
FROM window_double_test;

SELECT id, double_col,
  FIRST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_double,
  LAST_VALUE(double_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_double
FROM window_double_test;
