select p_mfgr, p_name, p_size,
min(p_retailprice),
rank() over(distribute by p_mfgr sort by p_name)as r,
dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
p_size, p_size - lag(p_size,-1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
from part
group by p_mfgr, p_name, p_size
;
