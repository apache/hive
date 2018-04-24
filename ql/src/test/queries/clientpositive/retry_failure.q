SET hive.vectorized.execution.enabled=false;
create table tx(a int,f string);
insert into tx values (1,'non_existent_file');

set zzz=1;
set reexec.overlay.zzz=2;

set hive.query.reexecution.enabled=true;
set hive.query.reexecution.strategies=overlay;

select assert_true(${hiveconf:zzz} > a) from tx group by a;

