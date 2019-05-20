--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join=true;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.fetch.task.conversion=none;

explain vectorization 
select count(*) from (select c.ctinyint 
from alltypesorc c
left outer join alltypesorc cd
  on cd.cint = c.cint 
left outer join alltypesorc hd
  on hd.ctinyint = c.ctinyint
) t1
;
select count(*) from (select c.ctinyint
from alltypesorc c
left outer join alltypesorc cd
  on cd.cint = c.cint 
left outer join alltypesorc hd
  on hd.ctinyint = c.ctinyint
) t1;

set hive.auto.convert.join=false;
set hive.vectorized.execution.enabled=false;
