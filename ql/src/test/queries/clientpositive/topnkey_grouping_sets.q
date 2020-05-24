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
SELECT a, b, grouping(a), grouping(b), grouping(a, b) FROM t_test_grouping_sets GROUP BY a, b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a, b LIMIT 3;
SELECT a, b, grouping(a), grouping(b), grouping(a, b) FROM t_test_grouping_sets GROUP BY a, b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a, b LIMIT 3;

set hive.optimize.topnkey=false;
SELECT a, b, grouping(a), grouping(b), grouping(a, b) FROM t_test_grouping_sets GROUP BY a, b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a, b LIMIT 3;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a, b LIMIT 10;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a, b LIMIT 10;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a, b LIMIT 10;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY b, a LIMIT 3;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY b, a LIMIT 3;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY b, a LIMIT 3;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY b, a LIMIT 1;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY b, a LIMIT 1;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a), (b), ()) ORDER BY b, a LIMIT 1;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a), (b)) ORDER BY b, a LIMIT 7;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a), (b)) ORDER BY b, a LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a), (b)) ORDER BY b, a LIMIT 7;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a)) ORDER BY a DESC, b ASC LIMIT 7;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a)) ORDER BY a DESC, b ASC LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (a)) ORDER BY a DESC, b ASC LIMIT 7;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (b)) ORDER BY a DESC, b ASC LIMIT 7;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (b)) ORDER BY a DESC, b ASC LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), (b)) ORDER BY a DESC, b ASC LIMIT 7;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), ()) ORDER BY a DESC, b ASC LIMIT 7;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), ()) ORDER BY a DESC, b ASC LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((a, b), ()) ORDER BY a DESC, b ASC LIMIT 7;

set hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((), (a, b)) ORDER BY a DESC, b ASC LIMIT 7;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((), (a, b)) ORDER BY a DESC, b ASC LIMIT 7;

set hive.optimize.topnkey=false;
SELECT a, b FROM t_test_grouping_sets GROUP BY a,b GROUPING SETS ((), (a, b)) ORDER BY a DESC, b ASC LIMIT 7;


set hive.optimize.topnkey=true;
EXPLAIN
SELECT tmp.a, tmp.b, max(tmp.c) FROM
  (SELECT a, b, c FROM t_test_grouping_sets GROUP BY a, b, c) tmp
GROUP BY tmp.a, tmp.b GROUPING SETS ((), (tmp.a,tmp.b)) ORDER BY tmp.a DESC, tmp.b ASC LIMIT 7;

SELECT tmp.a, tmp.b, max(tmp.c) FROM
  (SELECT a, b, c FROM t_test_grouping_sets GROUP BY a, b, c) tmp
GROUP BY tmp.a, tmp.b GROUPING SETS ((), (tmp.a,tmp.b)) ORDER BY tmp.a DESC, tmp.b ASC LIMIT 7;

set hive.optimize.topnkey=false;
SELECT tmp.a, tmp.b, max(tmp.c) FROM
  (SELECT a, b, c FROM t_test_grouping_sets GROUP BY a, b, c) tmp
GROUP BY tmp.a, tmp.b GROUPING SETS ((), (tmp.a,tmp.b)) ORDER BY tmp.a DESC, tmp.b ASC LIMIT 7;

DROP TABLE IF EXISTS t_test_grouping_sets;
