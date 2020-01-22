SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1431655765;
SET hive.vectorized.execution.enabled=false;


CREATE TABLE topnkey_windowing (tw_a string, tw_b string, tw_v1 double, tw_v2 double);
INSERT INTO topnkey_windowing VALUES
  (NULL, NULL, NULL, NULL),
  (NULL, 'D', 109, 9),
  ('A', 'D', 109, 9),
  ('A', 'D', 104, 9),
  ('A', 'D', 109, 9),
  ('A', 'C', 109, 9),
  ('A', 'C', 103, 9),
  (NULL, NULL, NULL, NULL),
  (NULL, 'D', 109, 9),
  ('A', 'D', 109, 9),
  ('A', 'D', 101, 9),
  ('A', 'D', 101, 9),
  ('A', 'D', 114, 9),
  ('A', 'D', 120, 9),
  ('B', 'E', 105, 9),
  ('B', 'E', 106, 9),
  ('B', 'E', 106, 9),
  ('B', 'E', NULL, NULL),
  ('B', 'E', 106, 9),
  ('A', 'C', 107, 9),
  ('B', 'E', 108, 9),
  ('A', 'C', 102, 9),
  ('B', 'E', 110, 9),
  (NULL, NULL, NULL, NULL),
  (NULL, NULL, 109, 9),
  ('A', 'D', 109, 9);

SET hive.optimize.topnkey=true;
EXPLAIN
SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a ORDER BY tw_v1 NULLS FIRST) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a ORDER BY tw_v1 NULLS FIRST) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.optimize.topnkey=false;
SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a ORDER BY tw_v1 NULLS FIRST) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;


SET hive.optimize.topnkey=true;
EXPLAIN
SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a ORDER BY tw_v1 ASC NULLS LAST, tw_v2 DESC NULLS FIRST) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a ORDER BY tw_v1 ASC NULLS LAST, tw_v2 DESC NULLS FIRST) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.optimize.topnkey=false;
SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a ORDER BY tw_v1 ASC NULLS LAST, tw_v2 DESC NULLS FIRST) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;


SET hive.optimize.topnkey=true;
EXPLAIN
SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a, tw_b ORDER BY tw_v1) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a, tw_b ORDER BY tw_v1) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

SET hive.optimize.topnkey=false;
SELECT tw_a, ranking
FROM (
  SELECT tw_a AS tw_a,
    rank() OVER (PARTITION BY tw_a, tw_b ORDER BY tw_v1) AS ranking
  FROM topnkey_windowing) tmp1
  WHERE ranking <= 3;

DROP TABLE topnkey_windowing;
