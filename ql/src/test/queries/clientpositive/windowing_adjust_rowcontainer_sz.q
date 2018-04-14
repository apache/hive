--! qt:dataset:part
set hive.join.cache.size=1;

select p_mfgr, p_name, p_size,
rank() over(distribute by p_mfgr sort by p_name) as r,
dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
from part
;

set hive.join.cache.size=25000;