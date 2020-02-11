--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table char_2_n0;

create table char_2_n0 (
  key char(10),
  value char(20)
) stored as orc;

insert overwrite table char_2_n0 select * from src;

-- Add a single NULL row that will come from ORC as isRepeated.
insert into char_2_n0 values (NULL, NULL);

select value, sum(cast(key as int)), count(*) numrows
from src
group by value
order by value asc
limit 5;

explain vectorization expression select value, sum(cast(key as int)), count(*) numrows
from char_2_n0
group by value
order by value asc
limit 5;

-- should match the query from src
select value, sum(cast(key as int)), count(*) numrows
from char_2_n0
group by value
order by value asc
limit 5;

select value, sum(cast(key as int)), count(*) numrows
from src
group by value
order by value desc
limit 5;

explain vectorization expression select value, sum(cast(key as int)), count(*) numrows
from char_2_n0
group by value
order by value desc
limit 5;

-- should match the query from src
select value, sum(cast(key as int)), count(*) numrows
from char_2_n0
group by value
order by value desc
limit 5;

drop table char_2_n0;
