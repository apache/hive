--! qt:dataset:part
-- =ALL is not allowed and initially triggers a CalciteSubquerySemanticException
set hive.cbo.fallback.strategy=TEST;
-- In TEST mode CalciteSubquerySemanticException is fatal
-- and should be present in the error message
explain select * from part where p_type = ALL(select max(p_type) from part);