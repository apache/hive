--! qt:dataset:src

set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;

EXPLAIN CBO
WITH materialized_cte AS (
  SELECT key, value FROM src WHERE key != '100'
),
another_materialized_cte AS (
  SELECT key, value FROM src WHERE key != '100'
)
SELECT a.key, a.value, b.key, b.value
FROM materialized_cte a
JOIN another_materialized_cte b ON a.key = b.key
ORDER BY a.key;

WITH materialized_cte AS (
  SELECT key, value FROM src WHERE key != '100'
),
another_materialized_cte AS (
  SELECT key, value FROM src WHERE key != '100'
)
SELECT a.key, a.value, b.key, b.value
FROM materialized_cte a
JOIN another_materialized_cte b ON a.key = b.key
ORDER BY a.key;
