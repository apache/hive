set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 17. SubQueries In
-- non agg, non corr
select * 
from src_cbo 
where src_cbo.key in (select key from src_cbo s1 where s1.key > '9') order by key
;

-- agg, corr
-- add back once rank issue fixed for cbo

-- distinct, corr
select * 
from src_cbo b 
where b.key in
        (select distinct a.key 
         from src_cbo a 
         where b.value = a.value and a.key > '9'
        ) order by b.key
;

-- non agg, corr, with join in Parent Query
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
;

-- where and having
-- Plan is:
-- Stage 1: b semijoin sq1:src_cbo (subquery in where)
-- Stage 2: group by Stage 1 o/p
-- Stage 5: group by on sq2:src_cbo (subquery in having)
-- Stage 6: Stage 2 o/p semijoin Stage 5
select key, value, count(*) 
from src_cbo b
where b.key in (select key from src_cbo where src_cbo.key > '8')
group by key, value
having count(*) in (select count(*) from src_cbo s1 where s1.key > '9' group by s1.key ) order by key
;

-- non agg, non corr, windowing
select p_mfgr, p_name, avg(p_size) 
from part 
group by p_mfgr, p_name
having p_name in 
  (select first_value(p_name) over(partition by p_mfgr order by p_size) from part) order by p_mfgr
;

