--! qt:dataset:src

SET hive.vectorized.execution.enabled=true;
create table tx(a int,f string);
insert into tx values (1,'non_existent_file');

set zzz=1;
set reexec.overlay.zzz=2;

set hive.query.reexecution.enabled=true;
set hive.query.reexecution.strategies=overlay;

explain vectorization expression
select assert_true(${hiveconf:zzz} > a) from tx group by a;
select assert_true(${hiveconf:zzz} > a) from tx group by a;
