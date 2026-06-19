--! qt:dataset:src
--! qt:dataset:part
--! qt:dataset:lineitem
set hive.cbo.enable=false;

-- non agg, non corr
explain rewrite
 select * 
from src 
where src.key in (select key from src s1 where s1.key > '9')
;

-- non agg, corr
explain rewrite
select * 
from src b 
where b.key in
        (select a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;

-- agg, non corr
explain rewrite
select p_name, p_size 
from 
part where part.p_size in 
	(select avg(p_size) 
	 from (select p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2
	)
;

-- agg, corr
explain rewrite
select p_mfgr, p_name, p_size 
from part b where b.p_size in 
	(select min(p_size) 
	 from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2 and b.p_mfgr = a.p_mfgr
	)
;

-- distinct, corr
explain rewrite
select * 
from src b 
where b.key in
        (select distinct a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;

-- non agg, non corr, windowing
explain rewrite
select p_mfgr, p_name, p_size 
from part 
where part.p_size in 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) from part)
;

-- non agg, non corr, with join in Parent Query
explain rewrite
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR')
;

-- non agg, corr, with join in Parent Query
explain rewrite
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
;
