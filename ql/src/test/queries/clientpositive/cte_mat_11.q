--! qt:dataset:src
set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;

EXPLAIN WITH materialized_cte1 AS (
  SELECT * FROM src
),
materialized_cte2 AS (
  SELECT a.key
  FROM materialized_cte1 a
  JOIN materialized_cte1 b ON (a.key = b.key)
)
SELECT a.key
FROM materialized_cte2 a
JOIN materialized_cte2 b ON (a.key = b.key);

EXPLAIN CBO WITH materialized_cte1 AS (
  SELECT * FROM src
),
materialized_cte2 AS (
  SELECT a.key
  FROM materialized_cte1 a
  JOIN materialized_cte1 b ON (a.key = b.key)
)
SELECT a.key
FROM materialized_cte2 a
JOIN materialized_cte2 b ON (a.key = b.key);

EXPLAIN WITH materialized_cte1 AS (
  SELECT * FROM src
),
materialized_cte2 AS (
  SELECT * FROM materialized_cte1
  UNION ALL
  SELECT * FROM materialized_cte1
)
SELECT * FROM materialized_cte2
UNION ALL
SELECT * FROM materialized_cte2;

EXPLAIN CBO WITH materialized_cte1 AS (
  SELECT * FROM src
),
materialized_cte2 AS (
  SELECT * FROM materialized_cte1
  UNION ALL
  SELECT * FROM materialized_cte1
)
SELECT * FROM materialized_cte2
UNION ALL
SELECT * FROM materialized_cte2;
