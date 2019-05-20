--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.optimize.limittranspose=true;
set hive.optimize.limittranspose.reductionpercentage=0.1f;
set hive.optimize.limittranspose.reductiontuples=100;
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.optimize.dynamic.partition.hashjoin=false;
set hive.mapred.mode=nonstrict;
explain
select a.*
from alltypesorc a left outer join src b
on a.cint = cast(b.key as int) and (a.cint < 100)
limit 1;


set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=20000;
set hive.exec.reducers.bytes.per.reducer=2000;

explain
select a.*
from alltypesorc a left outer join src b
on a.cint = cast(b.key as int) and (a.cint < 100)
limit 1;

explain
select a.*
from alltypesorc a left outer join src b
on a.cint = cast(b.key as int)
limit 1;
