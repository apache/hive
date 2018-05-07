--! qt:dataset:src
-- Distinct without an aggregation is unsupported

select hash(distinct key) from src;