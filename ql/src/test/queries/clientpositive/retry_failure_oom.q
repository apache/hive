
create table tx(a int,f string);
insert into tx values (1,'non_existent_file');

set zzz=1;
set reexec.overlay.zzz=2;

set hive.query.reexecution.enabled=true;
set hive.query.reexecution.strategies=overlay,reoptimize;

select assert_true_oom(${hiveconf:zzz} > a) from tx group by a;

