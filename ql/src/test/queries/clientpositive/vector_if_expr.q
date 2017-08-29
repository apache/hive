set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

EXPLAIN VECTORIZATION EXPRESSION
SELECT cboolean1, IF (cboolean1, 'first', 'second') FROM alltypesorc WHERE cboolean1 IS NOT NULL AND cboolean1 ORDER BY cboolean1;

SELECT cboolean1, IF (cboolean1, 'first', 'second') FROM alltypesorc WHERE cboolean1 IS NOT NULL AND cboolean1 ORDER BY cboolean1 LIMIT 5;

SELECT cboolean1, IF (cboolean1, 'first', 'second') FROM alltypesorc WHERE cboolean1 IS NOT NULL AND NOT cboolean1 ORDER BY cboolean1 LIMIT 5;