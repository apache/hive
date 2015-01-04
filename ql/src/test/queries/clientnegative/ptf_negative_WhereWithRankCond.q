-- testWhereWithRankCond
select  p_mfgr,p_name, p_size, 
rank() over() as r 
from part 
where r < 4 
distribute by p_mfgr 
sort by p_mfgr;
