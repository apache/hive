--! qt:dataset:part
--! qt:dataset:src
select p_name
from (select p_name from part distribute by 1 sort by 1) p 
distribute by 1 sort by 1
;

create temporary table d1 (key int);
create temporary table d2 (key int);

explain from (select key from src cluster by key) a
  insert overwrite table d1 select a.key
  insert overwrite table d2 select a.key cluster by a.key;