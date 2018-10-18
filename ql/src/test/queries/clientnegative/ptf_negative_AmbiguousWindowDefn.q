--! qt:dataset:part
-- testAmbiguousWindowDefn
select p_mfgr, p_name, p_size, 
sum(p_size) over (w1) as s1, 
sum(p_size) over (w2) as s2,
sum(p_size) over (w3) as s3
from part 
window w1 as (rows between 2 preceding and 2 following), 
       w2 as (rows between unbounded preceding and current row), 
       w3 as w3
distribute by p_mfgr 
sort by p_mfgr;

