--! qt:dataset:part

select t1.p_name from part t1 where t1.p_name IN (select t2.p_name from part t2 where t2.p_name IN
    (select max(t3.p_name) from part t3, part t4 where t3.p_name=t2.p_name and t3.p_name=t1.p_name))

