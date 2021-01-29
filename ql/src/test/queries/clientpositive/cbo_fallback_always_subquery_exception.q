--! qt:dataset:part
-- =ALL is not allowed and initially triggers a CalciteSubquerySemanticException
set hive.explain.user=true;
set hive.cbo.fallback.strategy=ALWAYS;
-- The query generates initially a CalciteSubquerySemanticException on CBO but can be handled by the legacy optimizer
-- It is not guaranteed that the resulting plan is correct whatsoever.
-- The fact that CBO fails should be reflected in the plan
explain select * from part where p_type = ALL(select max(p_type) from part);