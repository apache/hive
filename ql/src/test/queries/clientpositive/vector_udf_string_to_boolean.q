set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled = false;
SET hive.int.timestamp.conversion.in.seconds=false;
set hive.fetch.task.conversion=none;

create table t_n17 (s string) stored as orc;

insert into t_n17 values ('false');
insert into t_n17 values ('FALSE');
insert into t_n17 values ('FaLsE');
insert into t_n17 values ('true');
insert into t_n17 values ('TRUE');
insert into t_n17 values ('TrUe');
insert into t_n17 values ('');
insert into t_n17 values ('Other');
insert into t_n17 values ('Off');
insert into t_n17 values ('No');
insert into t_n17 values ('0');
insert into t_n17 values ('1');

explain select s,cast(s as boolean) from t_n17 order by s;

select s,cast(s as boolean) from t_n17 order by s;
