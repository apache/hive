set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 16. SubQueries Not In
-- non agg, non corr
select * 
from src_cbo 
where src_cbo.key not in  
  ( select key  from src_cbo s1 
    where s1.key > '2'
  ) order by key
;

-- non agg, corr
select p_mfgr, b.p_name, p_size 
from part b 
where b.p_name not in 
  (select p_name 
  from (select p_mfgr, p_name, p_size as r from part) a 
  where r < 10 and b.p_mfgr = a.p_mfgr 
  )
;

-- agg, non corr
select p_name, p_size 
from 
part where part.p_size not in 
  (select avg(p_size) 
  from (select p_size from part) a 
  where p_size < 10
  ) order by p_name
;

-- agg, corr
select p_mfgr, p_name, p_size 
from part b where b.p_size not in 
  (select min(p_size) 
  from (select p_mfgr, p_size from part) a 
  where p_size < 10 and b.p_mfgr = a.p_mfgr
  ) order by  p_name
;

-- non agg, non corr, Group By in Parent Query
select li.l_partkey, count(*) 
from lineitem li 
where li.l_linenumber = 1 and 
  li.l_orderkey not in (select l_orderkey from lineitem where l_shipmode = 'AIR') 
group by li.l_partkey
;

-- add null check test from sq_notin.q once HIVE-7721 resolved.

-- non agg, corr, having
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from (select p_mfgr, min(p_retailprice) l, max(p_retailprice) r, avg(p_retailprice) a from part group by p_mfgr) a 
  where min(p_retailprice) = l and r - l > 600
  )
  order by b.p_mfgr
;

-- agg, non corr, having
select b.p_mfgr, min(p_retailprice) 
from part b 
group by b.p_mfgr
having b.p_mfgr not in 
  (select p_mfgr 
  from part a
  group by p_mfgr
  having max(p_retailprice) - min(p_retailprice) > 600
  )
  order by b.p_mfgr  
;

