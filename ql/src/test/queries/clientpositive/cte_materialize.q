-- Test CTE materialization with both CBO enabled and disabled
-- Verifies DDLSemanticAnalyzerFactory is used for CTE materialization
-- Also ensures that an NPE is no longer triggered with CBO off (HIVE-28724 regression)

-- Test with CBO enabled (default)
explain
WITH cte AS (
    SELECT COUNT(*) as cnt FROM (SELECT 1 as id) t
)
SELECT * FROM cte
UNION ALL
SELECT * FROM cte
UNION ALL
SELECT * FROM cte;

-- Test with CBO disabled
set hive.cbo.enable=false;

explain
WITH cte AS (
    SELECT COUNT(*) as cnt FROM (SELECT 1 as id) t
)
SELECT * FROM cte
UNION ALL
SELECT * FROM cte
UNION ALL
SELECT * FROM cte;
