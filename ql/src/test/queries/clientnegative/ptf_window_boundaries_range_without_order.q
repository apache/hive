--! qt:dataset:part
select p_mfgr, p_name, p_size,
sum(p_retailprice) over (partition by p_mfgr range between 1 preceding and current row) as s1
from part;