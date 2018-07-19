--! qt:dataset:part
-- testJoinWithAmbigousAlias
select abc.* 
from noop(on part
partition by p_mfgr 
order by p_name 
) abc join part on abc.p_partkey = p1.p_partkey;
