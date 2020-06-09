set hive.vectorized.execution.enabled=false;
set hive.optimize.topnkey=true;

CREATE TABLE t_test_grouping_sets(
  a int,
  b int,
  c int
);

INSERT INTO t_test_grouping_sets VALUES
(NULL, NULL, NULL),
(5, 2, 3),
(10, 11, 12),
(NULL, NULL, NULL),
(NULL, NULL, NULL),
(6, 2, 1),
(7, 8, 4), (7, 8, 4), (7, 8, 4),
(5, 1, 2), (5, 1, 2), (5, 1, 2),
(NULL, NULL, NULL);

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b, sum(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b, a LIMIT 7;
SELECT a, b, sum(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b, a LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b, sum(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b, a LIMIT 7;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b, min(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((b,a), (a), (b), ()) ORDER BY b, a LIMIT 7;
SELECT a, b, min(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((b,a), (a), (b), ()) ORDER BY b, a LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b, min(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((b,a), (a), (b), ()) ORDER BY b, a LIMIT 7;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b, max(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b, a LIMIT 7;
SELECT a, b, max(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b, a LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b, max(c) FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a,b), (a), (b), ()) ORDER BY b, a LIMIT 7;

DROP TABLE IF EXISTS t_test_grouping_sets;
