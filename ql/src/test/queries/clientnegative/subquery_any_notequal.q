--! qt:dataset:part
-- <>ANY is not allowed
explain select * from part where p_type <> ANY(select max(p_type) from part);