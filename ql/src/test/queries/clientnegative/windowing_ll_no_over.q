--! qt:dataset:part
select p_mfgr, 
lead(p_retailprice,1) as s1  
from part;
