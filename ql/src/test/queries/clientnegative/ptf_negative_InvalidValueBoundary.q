set hive.mapred.mode=nonstrict;
-- testInvalidValueBoundary

select  p_mfgr,p_name, p_size,   
sum(p_size) over (w1) as s ,    
dense_rank() over(w1) as dr  
from part
window w1 as (partition by p_mfgr order by p_complex range between  2 preceding and current row);
