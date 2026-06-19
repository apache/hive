--! qt:dataset:part
select *
from part x 
where x.p_name in (select y.p_name from part y where exists (select z.p_name from part z where y.p_name = z.p_name))
;