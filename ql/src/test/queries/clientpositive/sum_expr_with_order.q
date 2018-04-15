--! qt:dataset:src
set hive.mapred.mode=nonstrict;

select 
cast(sum(key)*100 as decimal(15,3)) as c1
from src
order by c1;
