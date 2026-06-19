--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.stats.column.autogather=false;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table varchar_2_n0;

create table varchar_2_n0 (
  key varchar(10),
  value varchar(20)
) stored as orc;

insert overwrite table varchar_2_n0 select * from src;

select key, value
from src
order by key asc
limit 5;

explain vectorization select key, value
from varchar_2_n0
order by key asc
limit 5;

-- should match the query from src
select key, value
from varchar_2_n0
order by key asc
limit 5;

select key, value
from src
order by key desc
limit 5;

explain vectorization select key, value
from varchar_2_n0
order by key desc
limit 5;

-- should match the query from src
select key, value
from varchar_2_n0
order by key desc
limit 5;

drop table varchar_2_n0;

-- Implicit conversion.  Occurs in reduce-side under Tez.
create table varchar_3 (
  field varchar(25)
) stored as orc;

explain vectorization expression
insert into table varchar_3 select cint from alltypesorc limit 10;

insert into table varchar_3 select cint from alltypesorc limit 10;

drop table varchar_3;
