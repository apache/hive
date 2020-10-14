set hive.spark.stage.max.tasks=1;

add file ../../data/scripts/sleep.py;

EXPLAIN
SELECT TRANSFORM(key) USING 'python sleep.py' AS k
  FROM (SELECT key FROM src1 GROUP BY key) a ORDER BY k;

SELECT TRANSFORM(key) USING 'python sleep.py' AS k
  FROM (SELECT key FROM src1 GROUP BY key) a ORDER BY k;
