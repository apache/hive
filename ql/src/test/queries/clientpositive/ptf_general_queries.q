-- SORT_QUERY_RESULTS

-- 1. testNoPTFNoWindowing
select p_mfgr, p_name, p_size
from part
distribute by p_mfgr
sort by p_name ;
        
-- 2. testUDAFsNoWindowingNoPTFNoGBY
select p_mfgr,p_name, p_retailprice,  
sum(p_retailprice) over(partition by p_mfgr order by p_name) as s,
min(p_retailprice) over(partition by p_mfgr order by p_name) as mi,
max(p_retailprice) over(partition by p_mfgr order by p_name) as ma,
avg(p_retailprice) over(partition by p_mfgr order by p_name) as av 
from part 
;        
        
-- 3. testConstExprInSelect
select 'tst1' as key, count(1) as value from part;
