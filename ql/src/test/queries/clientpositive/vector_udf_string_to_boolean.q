set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled = false;
SET hive.int.timestamp.conversion.in.seconds=false;
set hive.fetch.task.conversion=none;

create table t (s string) stored as orc;

insert into t values ('false');
insert into t values ('FALSE');
insert into t values ('FaLsE');
insert into t values ('true');
insert into t values ('TRUE');
insert into t values ('TrUe');
insert into t values ('');
insert into t values ('Other');
insert into t values ('Off');
insert into t values ('No');
insert into t values ('0');
insert into t values ('1');

explain select s,cast(s as boolean) from t order by s;

select s,cast(s as boolean) from t order by s;
