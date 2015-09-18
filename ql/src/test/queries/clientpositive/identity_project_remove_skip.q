set hive.optimize.remove.identity.project=true;
set hive.auto.convert.join=true;
set hive.optimize.ppd=true;
set hive.explain.user=false;

explain
select t2.* 
from
  (select key,value from (select key,value from src) t1 sort by key) t2
  join 
  (select * from src sort by key) t3 
  on (t2.key=t3.key )
  where t2.value='val_105' and t3.key='105';
  
select t2.* 
from
  (select key,value from (select key,value from src) t1 sort by key) t2
  join 
  (select * from src sort by key) t3 
  on (t2.key=t3.key )
  where t2.value='val_105' and t3.key='105';
