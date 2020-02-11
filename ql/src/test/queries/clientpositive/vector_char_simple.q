--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.stats.column.autogather=false;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table char_2;

create table char_2 (
  key char(10),
  value char(20)
) stored as orc;

insert overwrite table char_2 select * from src;

select key, value
from src
order by key asc
limit 5;

explain vectorization only select key, value
from char_2
order by key asc
limit 5;

-- should match the query from src
select key, value
from char_2
order by key asc
limit 5;

select key, value
from src
order by key desc
limit 5;

explain vectorization only select key, value
from char_2
order by key desc
limit 5;

-- should match the query from src
select key, value
from char_2
order by key desc
limit 5;

drop table char_2;


-- Implicit conversion.  Occurs in reduce-side under Tez.
create table char_3 (
  field char(12)
) stored as orc;

explain vectorization only operator
insert into table char_3 select cint from alltypesorc limit 10;

insert into table char_3 select cint from alltypesorc limit 10;

drop table char_3;
