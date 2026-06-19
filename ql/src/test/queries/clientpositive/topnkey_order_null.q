SET hive.vectorized.execution.enabled=false;
SET hive.optimize.topnkey=true;

CREATE TABLE t_test(
  a int,
  b int,
  c int
);

INSERT INTO t_test VALUES
(NULL, NULL, NULL),
(5, 2, 3),
(NULL, NULL, NULL),
(NULL, NULL, NULL),
(6, 2, 1),
(7, 8, 4), (7, 8, 4), (7, 8, 4),
(5, 1, 2), (5, 1, 2), (5, 1, 2),
(NULL, NULL, NULL);

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a LIMIT 2;

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a NULLS FIRST LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a NULLS FIRST LIMIT 2;

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a NULLS LAST LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a NULLS LAST LIMIT 2;

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC LIMIT 2;

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC NULLS FIRST LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC NULLS FIRST LIMIT 2;

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC NULLS LAST LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC NULLS LAST LIMIT 2;

DROP TABLE IF EXISTS t_test;

CREATE TABLE t_test(
  a int,
  b int,
  c int
);

INSERT INTO t_test VALUES
(7, 8, 4), (7, 8, 4), (7, 8, 4),
(NULL, NULL, NULL),
(5, 2, 3),
(NULL, NULL, NULL),
(NULL, NULL, NULL),
(6, 2, 1),
(5, 1, 2), (5, 1, 2), (5, 1, 2),
(NULL, NULL, NULL);

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC LIMIT 2;

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC NULLS FIRST LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC NULLS FIRST LIMIT 2;

SET hive.vectorized.execution.enabled=false;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC NULLS LAST LIMIT 2;
SET hive.vectorized.execution.enabled=true;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC NULLS LAST LIMIT 2;

DROP TABLE IF EXISTS t_test;
