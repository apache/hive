
create temporary function mylastval as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLastValue';

select  p_mfgr,p_name, p_size, 
sum(p_size) over (distribute by p_mfgr sort by p_name rows between current row and current row) as s2, 
first_value(p_size) over w1  as f, 
last_value(p_size, false) over w1  as l,
mylastval(p_size, false) over w1  as m 
from part 
window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following);

