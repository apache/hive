--! qt:dataset:part
-- testPartitonBySortBy
select p_mfgr, p_name, p_size,
sum(p_retailprice) over (distribute by p_mfgr order by p_mfgr) as s1
from part 
;
