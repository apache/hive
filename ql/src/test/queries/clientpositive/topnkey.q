--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=false;
set hive.optimize.topnkey=true;

set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.tez.min.bloom.filter.entries=1;

set hive.tez.dynamic.partition.pruning=true;
set hive.stats.fetch.column.stats=true;
set hive.cbo.enable=true;

EXPLAIN EXTENDED
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

EXPLAIN
SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;

SELECT key FROM src GROUP BY key ORDER BY key LIMIT 5;

explain vectorization detail
SELECT src1.key, src2.value FROM src src1 JOIN src src2 ON (src1.key = src2.key) ORDER BY src1.key LIMIT 5;

SELECT src1.key, src2.value FROM src src1 JOIN src src2 ON (src1.key = src2.key) ORDER BY src1.key LIMIT 5;

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

EXPLAIN
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;


EXPLAIN
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a LIMIT 2;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a LIMIT 2;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a NULLS FIRST LIMIT 2;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a NULLS LAST LIMIT 2;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC LIMIT 2;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a ASC NULLS FIRST LIMIT 2;
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

SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC LIMIT 2;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC NULLS FIRST LIMIT 2;
SELECT a, count(b) FROM t_test GROUP BY a ORDER BY a DESC NULLS LAST LIMIT 2;

DROP TABLE IF EXISTS t_test;
