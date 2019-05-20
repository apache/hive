set hive.cbo.enable=false;
set hive.compute.query.using.stats=true;

EXPLAIN
SELECT round(year(to_date(current_date())),-3) as GROUP_BY_FIELD, count (*)  as src_cnt
from src
WHERE 1=1
group by round(year(to_date(current_date())),-3);

SELECT round(year(to_date(current_date())),-3) as GROUP_BY_FIELD, count (*)  as src_cnt
from src
WHERE 1=1
group by round(year(to_date(current_date())),-3);

