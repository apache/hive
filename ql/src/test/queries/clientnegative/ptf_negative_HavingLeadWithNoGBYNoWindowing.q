-- testHavingLeadWithNoGBYNoWindowing
select  p_mfgr,p_name, p_size 
from part 
having lead(p_size, 1) over() <= p_size 
distribute by p_mfgr 
sort by p_name;
