set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=false;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

EXPLAIN VECTORIZATION EXPRESSION  SELECT COUNT(t1.cint) AS CNT, MAX(t2.cint) , MIN(t1.cint), AVG(t1.cint+t2.cint)
  FROM alltypesorc t1
  JOIN alltypesorc t2 ON t1.cint = t2.cint order by CNT;

SELECT COUNT(t1.cint), MAX(t2.cint) AS CNT, MIN(t1.cint), AVG(t1.cint+t2.cint)
  FROM alltypesorc t1
  JOIN alltypesorc t2 ON t1.cint = t2.cint order by CNT;