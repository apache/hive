SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1431655765;


CREATE TABLE topnkey_windowing (tw_code string, tw_value double);
INSERT INTO topnkey_windowing VALUES
  (NULL, NULL),
  (NULL, 109),
  ('A', 109),
  ('A', 104),
  ('A', 109),
  ('A', 109),
  ('A', 103),
  (NULL, NULL),
  (NULL, 109),
  ('A', 109),
  ('A', 101),
  ('A', 101),
  ('A', 114),
  ('A', 120),
  ('B', 105),
  ('B', 106),
  ('B', 106),
  ('B', NULL),
  ('B', 106),
  ('A', 107),
  ('B', 108),
  ('A', 102),
  ('B', 110),
  (NULL, NULL),
  (NULL, 109),
  ('A', 109);

SET hive.optimize.topnkey=true;
SET hive.vectorized.execution.enabled=false;
EXPLAIN
SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.vectorized.execution.enabled=true;
EXPLAIN
SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.optimize.topnkey=false;
SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;


SET hive.optimize.topnkey=true;
SET hive.vectorized.execution.enabled=false;
EXPLAIN extended
SELECT tw_code, ranking
FROM (
  SELECT tw_code as tw_code,
    rank() OVER (ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_code, ranking
FROM (
  SELECT tw_code as tw_code,
    rank() OVER (ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.vectorized.execution.enabled=true;
EXPLAIN extended
SELECT tw_code, ranking
FROM (
  SELECT tw_code as tw_code,
    rank() OVER (ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_code, ranking
FROM (
  SELECT tw_code as tw_code,
    rank() OVER (ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.optimize.topnkey=false;
SELECT tw_code, ranking
FROM (
  SELECT tw_code as tw_code,
    rank() OVER (ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;



SET hive.optimize.topnkey=true;
SET hive.vectorized.execution.enabled=true;
EXPLAIN
SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    dense_rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    dense_rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.optimize.topnkey=false;
SELECT tw_code, ranking
FROM (
  SELECT tw_code AS tw_code,
    dense_rank() OVER (PARTITION BY tw_code ORDER BY tw_value) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

DROP TABLE topnkey_windowing;
