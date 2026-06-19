--! qt:dataset:src
select distinct key, explode(key) from src;
