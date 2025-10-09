--! qt:dataset:src
set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;

-- Test cases with full stats
DESCRIBE FORMATTED src key;
DESCRIBE FORMATTED src value;

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

-- Test cases with no stats
set hive.stats.autogather=false;
CREATE TABLE src_no_stats AS SELECT * FROM src;
DESCRIBE FORMATTED src_no_stats key;
DESCRIBE FORMATTED src_no_stats value;

EXPLAIN WITH materialized_cte1 AS (
  SELECT * FROM src_no_stats
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
  SELECT * FROM src_no_stats
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
  SELECT * FROM src_no_stats
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
  SELECT * FROM src_no_stats
),
materialized_cte2 AS (
  SELECT * FROM materialized_cte1
  UNION ALL
  SELECT * FROM materialized_cte1
)
SELECT * FROM materialized_cte2
UNION ALL
SELECT * FROM materialized_cte2;

-- Test cases with partial stats(only key)
CREATE TABLE src_partial_stats_key AS SELECT * FROM src;
ANALYZE TABLE src_partial_stats_key COMPUTE STATISTICS FOR COLUMNS key;
DESCRIBE FORMATTED src_partial_stats_key key;
DESCRIBE FORMATTED src_partial_stats_key value;

EXPLAIN WITH materialized_cte1 AS (
  SELECT * FROM src_partial_stats_key
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
  SELECT * FROM src_partial_stats_key
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
  SELECT * FROM src_partial_stats_key
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
  SELECT * FROM src_partial_stats_key
),
materialized_cte2 AS (
  SELECT * FROM materialized_cte1
  UNION ALL
  SELECT * FROM materialized_cte1
)
SELECT * FROM materialized_cte2
UNION ALL
SELECT * FROM materialized_cte2;

-- Test cases with partial stats(only value)
CREATE TABLE src_partial_stats_value AS SELECT * FROM src;
ANALYZE TABLE src_partial_stats_value COMPUTE STATISTICS FOR COLUMNS value;
DESCRIBE FORMATTED src_partial_stats_value key;
DESCRIBE FORMATTED src_partial_stats_value value;

EXPLAIN WITH materialized_cte1 AS (
  SELECT * FROM src_partial_stats_value
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
  SELECT * FROM src_partial_stats_value
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
  SELECT * FROM src_partial_stats_value
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
  SELECT * FROM src_partial_stats_value
),
materialized_cte2 AS (
  SELECT * FROM materialized_cte1
  UNION ALL
  SELECT * FROM materialized_cte1
)
SELECT * FROM materialized_cte2
UNION ALL
SELECT * FROM materialized_cte2;
