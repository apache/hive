--! qt:dataset:part
-- testPartitonBySortBy
select p_mfgr, p_name, p_size,
sum(p_retailprice) over (partition by p_mfgr sort by p_mfgr) as s1
from part
;
