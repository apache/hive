set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.cbo.enable=true;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.vectorized.execution.mapjoin.native.fast.hashtable.enabled=true;

create temporary table x (a int) stored as orc;
create temporary table y (b int) stored as orc;
insert into x values(1);
insert into y values(1);

explain vectorization expression
select count(1) from x, y where a = b;

select count(1) from x, y where a = b;
