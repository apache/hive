--! qt:dataset:part
-- testHavingLeadWithPTF
select  p_mfgr,p_name, p_size 
from noop(on part 
partition by p_mfgr 
order by p_name) 
having lead(p_size, 1) over() <= p_size 
distribute by p_mfgr 
sort by p_name;   
