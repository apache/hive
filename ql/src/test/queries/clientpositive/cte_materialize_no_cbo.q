-- HIVE-28724 regression: SemanticAnalyzer.materializeCTE uses wrong analyzer class
-- CalcitePlanner.materializeCTE was fixed to use CreateTableAnalyzer,
-- but SemanticAnalyzer.materializeCTE still uses SemanticAnalyzer directly.
-- Bug triggers when CBO is disabled

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
