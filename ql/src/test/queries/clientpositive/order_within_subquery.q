--! qt:dataset:part
select t1.p_name, t2.p_name 
from (select * from part order by p_size limit 10) t1 join part t2 on t1.p_partkey = t2.p_partkey and t1.p_size = t2.p_size 
where t1.p_partkey < 100000;

