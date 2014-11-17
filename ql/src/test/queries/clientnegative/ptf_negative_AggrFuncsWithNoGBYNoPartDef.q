-- testAggrFuncsWithNoGBYNoPartDef
select p_mfgr, 
sum(p_retailprice) as s1  
from part;