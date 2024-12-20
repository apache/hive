--! qt:dataset:src
SET hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=false;
SET hive.optimize.topnkey=true;

SET hive.optimize.ppd=true;
SET hive.ppd.remove.duplicatefilters=true;
SET hive.tez.dynamic.partition.pruning=true;
SET hive.optimize.metadataonly=false;
SET hive.optimize.index.filter=true;
SET hive.tez.min.bloom.filter.entries=1;

SET hive.stats.fetch.column.stats=true;
SET hive.cbo.enable=true;

SET hive.optimize.topnkey=true;
EXPLAIN
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

SET hive.optimize.topnkey=false;
SELECT key, SUM(CAST(SUBSTR(value,5) AS INT)) FROM src GROUP BY key ORDER BY key LIMIT 5;

SET hive.optimize.topnkey=true;
EXPLAIN
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

SET hive.optimize.topnkey=false;
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key LIMIT 5;

SET hive.optimize.topnkey=true;
EXPLAIN
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key NULLS FIRST LIMIT 5;
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key NULLS FIRST LIMIT 5;

SET hive.optimize.topnkey=false;
SELECT src1.key, src2.value FROM src src1 LEFT OUTER JOIN src src2 ON (src1.key = src2.key) GROUP BY src1.key, src2.value ORDER BY src1.key NULLS FIRST LIMIT 5;

CREATE TABLE t_test(
  a int,
  b int,
  c int
);

INSERT INTO t_test VALUES
(5, 2, 3),
(6, 2, 1),
(7, 8, 4), (7, 8, 4), (7, 8, 4),
(5, 1, 2), (5, 1, 2), (5, 1, 2);

SET hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test ORDER BY a, b LIMIT 3;
SELECT a, b FROM t_test ORDER BY a, b LIMIT 3;

SET hive.optimize.topnkey=false;
SELECT a, b FROM t_test ORDER BY a, b LIMIT 3;

SET hive.optimize.topnkey=true;
EXPLAIN
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;

SET hive.optimize.topnkey=false;
SELECT a, b FROM t_test GROUP BY a, b ORDER BY a, b LIMIT 3;

SET hive.optimize.topnkey=true;
EXPLAIN
SELECT a, count(distinct b), min(c) FROM t_test GROUP BY a ORDER BY a LIMIT 3;
SELECT a, count(distinct b), min(c) FROM t_test GROUP BY a ORDER BY a LIMIT 3;

SET hive.optimize.topnkey=false;
SELECT a, count(distinct b), min(c) FROM t_test GROUP BY a ORDER BY a LIMIT 3;

DROP TABLE t_test;
