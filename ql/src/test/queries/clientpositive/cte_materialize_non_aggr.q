-- HIVE-28724 regression: SemanticAnalyzer.materializeCTE uses wrong analyzer class
-- CalcitePlanner.materializeCTE was fixed to use CreateTableAnalyzer,
-- but SemanticAnalyzer.materializeCTE still uses SemanticAnalyzer directly.
-- Bug triggers when: CBO disabled + non-aggregate CTE materialization

set hive.optimize.cte.materialize.full.aggregate.only=false;
set hive.cbo.enable=false;

explain
WITH cte AS (
    SELECT 1 as id
)
SELECT * FROM cte
UNION ALL
SELECT * FROM cte
UNION ALL
SELECT * FROM cte;
