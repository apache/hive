set hive.spark.stage.max.tasks=1;

EXPLAIN
SELECT key, sum(value) AS s FROM src1 GROUP BY key ORDER BY s;

SELECT key, sum(value) AS s FROM src1 GROUP BY key ORDER BY s;
