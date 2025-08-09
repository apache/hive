set hive.cli.print.header=true;

CREATE TABLE window_int_test (
  id INT,
  int_col INT
);

INSERT INTO window_int_test VALUES
  (1, NULL),
  (2, NULL),
  (1, NULL),
  (1, NULL),
  (2, 20),
  (3, NULL),
  (6, 62),
  (2, NULL),
  (3, 30),
  (4, NULL),
  (3, 32),
  (5, 50),
  (4, 40),
  (5, NULL),
  (6, 60),
  (7, NULL),
  (NULL, 80),
  (NULL, NULL);

-- ================ Test FIRST_VALUE and LAST_VALUE for int column ====================================================

-- Test FIRST_VALUE and LAST_VALUE for int column with IGNORE NULLS outside function
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column with IGNORE NULLS inside function
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column with PARTITION BY clause
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col) OVER(ORDER BY id) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col) OVER(ORDER BY id) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column with PARTITION BY clause and without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id) AS first_int,
  LAST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id) AS first_int,
  LAST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id) AS last_int
FROM window_int_test;


--========================================================================================
-- Repeat the same tests with nulls first added after order by clause
--========================================================================================
-- Test FIRST_VALUE and LAST_VALUE for int column with IGNORE NULLS outside function
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column with IGNORE NULLS inside function
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column with PARTITION BY clause
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

-- Test FIRST_VALUE and LAST_VALUE for int column with PARTITION BY clause and without IGNORE NULLS
set hive.vectorized.execution.enabled=false;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

set hive.vectorized.execution.enabled=true;
SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

--========================================================================================
-- Test with reducer batch size set to 2
--========================================================================================
set hive.vectorized.testing.reducer.batch.size=2;
set hive.vectorized.execution.enabled=true;

SELECT id, int_col,
  FIRST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) IGNORE NULLS OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col IGNORE NULLS) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(ORDER BY id) AS first_int,
  LAST_VALUE(int_col) OVER(ORDER BY id) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) OVER(ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id) AS first_int,
  LAST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id) AS last_int
FROM window_int_test;

SELECT id, int_col,
  FIRST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS first_int,
  LAST_VALUE(int_col) OVER(PARTITION BY id ORDER BY id ASC NULLS FIRST) AS last_int
FROM window_int_test;
