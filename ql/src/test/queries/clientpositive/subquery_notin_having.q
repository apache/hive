-- non agg, non corr
-- JAVA_VERSION_SPECIFIC_OUTPUT

explain
select key, count(*) 
from src 
group by key
having key not in  
  ( select key  from src s1 
    where s1.key > '12'
  )
;

-- non agg, corr
explain
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from part group by p_mfgr) a 
  where min(p_retailprice) = l and r - l > 600
  )
;

select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from part group by p_mfgr) a 
  where min(p_retailprice) = l and r - l > 600
  )
;

-- agg, non corr
explain
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from part a
  group by p_mfgr
  having max(p_retailprice) - min(p_retailprice) > 600
  )
;

select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from part a
  group by p_mfgr
  having max(p_retailprice) - min(p_retailprice) > 600
  )
;
