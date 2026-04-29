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

-- Test the recompile-without-CBO auto-trigger path.
-- With hive.cbo.fallback.strategy=ALWAYS, a CBO crash on `= ALL (subquery)`
-- causes ReCompileWithoutCBOPlugin to set hive.cbo.enable=false in conf and
-- recompile. The recompile constructs a plain SemanticAnalyzer via the factory,
-- and must materialize the CTE through the fixed SemanticAnalyzer.materializeCTE
-- path. Without the fix this path NPEs at SemanticAnalyzer.materializeCTE.
set hive.cbo.enable=true;
set hive.cbo.fallback.strategy=ALWAYS;

explain
WITH cte AS (
    SELECT MAX(s) AS m FROM (SELECT 'a' AS s) t
)
SELECT s FROM (SELECT 'a' AS s) u WHERE s = ALL(SELECT m FROM cte)
UNION ALL
SELECT s FROM (SELECT 'a' AS s) u WHERE s = ALL(SELECT m FROM cte)
UNION ALL
SELECT s FROM (SELECT 'a' AS s) u WHERE s = ALL(SELECT m FROM cte);
