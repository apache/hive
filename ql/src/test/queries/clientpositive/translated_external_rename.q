set hive.fetch.task.conversion=none;
set hive.compute.query.using.stats=false;

create table t (a integer);
insert into t values(1);
alter table t rename to t2;
create table t (a integer); -- I expected an exception from this command (location already exists) but because its an external table no exception
insert into t values(2);
select * from t;  -- shows 1 and 2
drop table t2;    -- wipes out data location
select * from t;  -- empty resultset

