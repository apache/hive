--! qt:dataset:src
set hive.cbo.fallback.strategy=CONSERVATIVE;
select cast(key as bigint) = '1'
from src
qualify row_number() over (partition by key order by value) = 1;
