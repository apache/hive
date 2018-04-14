--! qt:dataset:part
set hive.mapred.mode=nonstrict;

explain select p1.p_size, p2.p_size 
from part p1 left outer join part p2 on p1.p_partkey = p2.p_partkey 
  right outer join part p3 on p2.p_partkey = p3.p_partkey and 
              p1.p_size > 10
;

explain select p1.p_size, p2.p_size 
from part p1 left outer join part p2 on p1.p_partkey = p2.p_partkey 
  right outer join part p3 on p2.p_partkey = p3.p_partkey and 
              p1.p_size > 10 and p1.p_size > p2.p_size + 10
;