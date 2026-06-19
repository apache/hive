--! qt:dataset:part
-- corr and windowing 
select p_mfgr, p_name, p_size 
from part a 
where a.p_size in 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) 
   from part b 
   where a.p_brand = b.p_brand)
;