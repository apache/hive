
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=1300000;
set hive.mapjoin.optimized.hashtable.wbsize=880000;
set hive.mapjoin.hybridgrace.memcheckfrequency=1024;

set hive.mapjoin.hybridgrace.hashtable=false;

explain vectorization expression
select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint
 where c.cint < 2000000000) t1
;

set hive.mapjoin.hybridgrace.hashtable=true;
set hive.llap.enable.grace.join.in.llap=false;

-- The vectorized mapjoin className/nativeConditionsMet should be the same as the previous query
explain vectorization expression
select count(*) from
(select c.ctinyint
 from alltypesorc c
 inner join alltypesorc cd
 on cd.cint = c.cint
 where c.cint < 2000000000) t1
;
