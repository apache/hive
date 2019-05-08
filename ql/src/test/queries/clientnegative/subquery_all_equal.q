--! qt:dataset:part
-- =ALL is not allowed
explain select * from part where p_type = ALL(select max(p_type) from part);