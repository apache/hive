set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
drop table varchar_2;

create table varchar_2 (
  key varchar(10),
  value varchar(20)
) stored as orc;

insert overwrite table varchar_2 select * from src;

select key, value
from src
order by key asc
limit 5;

explain select key, value
from varchar_2
order by key asc
limit 5;

-- should match the query from src
select key, value
from varchar_2
order by key asc
limit 5;

select key, value
from src
order by key desc
limit 5;

explain select key, value
from varchar_2
order by key desc
limit 5;

-- should match the query from src
select key, value
from varchar_2
order by key desc
limit 5;

drop table varchar_2;

-- Implicit conversion.  Occurs in reduce-side under Tez.
create table varchar_3 (
  field varchar(25)
) stored as orc;

explain
insert into table varchar_3 select cint from alltypesorc limit 10;

insert into table varchar_3 select cint from alltypesorc limit 10;

drop table varchar_3;
