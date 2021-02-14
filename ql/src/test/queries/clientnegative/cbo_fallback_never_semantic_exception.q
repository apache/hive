--! qt:dataset:src
set hive.cbo.fallback.strategy=NEVER;
-- The query generates initially a CalciteSemanticException on CBO but can be handled by the legacy optimizer
-- In NEVER mode we never fallback so the error should contain the CalciteSemanticException
select count(*) from src where key <=> 100;