set hive.mapred.mode=nonstrict;
explain select *
from part p1 join part p2 join part p3 
where p1.p_name = p2.p_name and p2.p_name = p3.p_name;

explain select *
from part p1 join part p2 join part p3 
where p2.p_name = p1.p_name and p3.p_name = p2.p_name;

explain select *
from part p1 join part p2 join part p3 
where p2.p_partkey + p1.p_partkey = p1.p_partkey and p3.p_name = p2.p_name;

explain select *
from part p1 join part p2 join part p3 
where p2.p_partkey = 1 and p3.p_name = p2.p_name;
