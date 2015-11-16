set hive.optimize.limitjointranspose=true;
set hive.optimize.limitjointranspose.reductionpercentage=0.1f;
set hive.optimize.limitjointranspose.reductiontuples=100;
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.optimize.dynamic.partition.hashjoin=false;

explain
select a.*
from alltypesorc a left outer join src b
on a.cint = cast(b.key as int) and (a.cint < 100)
limit 1;


set hive.auto.convert.join=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join.noconditionaltask.size=200000;
set hive.exec.reducers.bytes.per.reducer=200000;

explain
select a.*
from alltypesorc a left outer join src b
on a.cint = cast(b.key as int) and (a.cint < 100)
limit 1;
