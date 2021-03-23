--! qt:dataset:src
set hive.explain.user=true;
set hive.cbo.fallback.strategy=CONSERVATIVE;
-- The query generates initially a CalciteSemanticException on CBO but can be handled by the legacy optimizer
-- The fact that CBO fails should be reflected in the plan
explain select count(*) from src where key <=> 100;