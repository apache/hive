--! qt:dataset:part
-- =ALL is not allowed and initially triggers a CalciteSubquerySemanticException
set hive.cbo.fallback.strategy=ALWAYS;
set hive.query.reexecution.strategies=overlay,reoptimize,reexecute_lost_am,dagsubmit;
-- In ALWAYS mode CalciteSubquerySemanticException should be retried but the wrong configuration should prevent it
explain select * from part where p_type = ALL(select max(p_type) from part);