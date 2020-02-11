--! qt:dataset:part
-- testNoWindowDefn
select p_mfgr, p_name, p_size,
sum(p_size) over (w1) as s1,
sum(p_size) over (w2) as s2
from part
window w1 as (rows between 2 preceding and 2 following)
distribute by p_mfgr
sort by p_mfgr;

