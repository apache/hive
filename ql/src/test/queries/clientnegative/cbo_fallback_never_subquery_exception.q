--! qt:dataset:part
-- =ALL is not allowed and initially triggers a CalciteSubquerySemanticException
-- In NEVER mode we don't retry on CBO failure so CalciteSubquerySemanticException should appear in the error
explain select * from part where p_type = ALL(select max(p_type) from part);