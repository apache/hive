set hive.cbo.enable=false;
set hive.compute.query.using.stats=true;

EXPLAIN
SELECT to_date(current_date()) as GROUP_BY_FIELD, count (*)  as src_cnt
from src
WHERE 1=1
group by to_date(current_date());

SELECT to_date(current_date()) as GROUP_BY_FIELD, count (*)  as src_cnt
from src
WHERE 1=1
group by to_date(current_date());

