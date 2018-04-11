set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

EXPLAIN VECTORIZATION DETAIL
SELECT SUM(abs(ctinyint)) from alltypesorc;
SELECT SUM(abs(ctinyint)) from alltypesorc;

