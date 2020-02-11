--! qt:dataset:part
set hive.mapred.mode=nonstrict;
explain select *
from part p1 join part p2 join part p3 on p1.p_name = p2.p_name join part p4 on p2.p_name = p3.p_name and p1.p_name = p4.p_name;

explain select *
from part p1 join part p2 join part p3 on p2.p_name = p1.p_name join part p4 on p2.p_name = p3.p_name and p1.p_partkey = p4.p_partkey 
            and p1.p_partkey = p2.p_partkey;
