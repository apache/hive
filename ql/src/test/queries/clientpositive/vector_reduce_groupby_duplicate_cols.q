set hive.cli.print.header=true;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.reducesink.new.enabled=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
set hive.fetch.task.conversion=none;
set hive.strict.checks.cartesian.product=false;
set hive.cbo.enable=false;

-- HIVE-18258

create table demo (one int, two int);
insert into table demo values (1, 2);

explain vectorization detail
select one as one_0, two, one as one_1
from demo a
join (select 1 as one, 2 as two) b
on a.one = b.one and a.two = b.two
group by a.one, a.two, a.one;

select one as one_0, two, one as one_1
from demo a
join (select 1 as one, 2 as two) b
on a.one = b.one and a.two = b.two
group by a.one, a.two, a.one;