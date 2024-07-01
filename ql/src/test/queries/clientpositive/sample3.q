--! qt:dataset:srcbucket
-- SORT_QUERY_RESULTS

-- no input pruning, sample filter
set hive.cbo.fallback.strategy=NEVER;

EXPLAIN
SELECT s.key
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s;

SELECT s.key
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s SORT BY key;

