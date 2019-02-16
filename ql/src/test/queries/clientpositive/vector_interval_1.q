-- SORT_QUERY_RESULTS
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.cli.print.header=true;

drop table if exists vector_interval_1;
create table vector_interval_1 (ts timestamp, dt date, str1 string, str2 string) stored as orc;

insert into vector_interval_1
  select timestamp '2001-01-01 01:02:03', date '2001-01-01', '1-2', '1 2:3:4' from src limit 1;
insert into vector_interval_1
  select null, null, null, null from src limit 1;

select * from vector_interval_1;

-- constants/cast from string
explain vectorization expression
select
  str1,
  interval '1-2' year to month, interval_year_month(str1),
  interval '1 2:3:4' day to second, interval_day_time(str2)
from vector_interval_1 order by str1;

select
  str1,
  interval '1-2' year to month, interval_year_month(str1),
  interval '1 2:3:4' day to second, interval_day_time(str2)
from vector_interval_1 order by str1;


-- interval arithmetic
explain vectorization expression
select
  dt,
  interval '1-2' year to month + interval '1-2' year to month,
  interval_year_month(str1) + interval_year_month(str1),
  interval '1-2' year to month + interval_year_month(str1),
  interval '1-2' year to month - interval '1-2' year to month,
  interval_year_month(str1) - interval_year_month(str1),
  interval '1-2' year to month - interval_year_month(str1)
from vector_interval_1 order by dt;

select
  dt,
  interval '1-2' year to month + interval '1-2' year to month,
  interval_year_month(str1) + interval_year_month(str1),
  interval '1-2' year to month + interval_year_month(str1),
  interval '1-2' year to month - interval '1-2' year to month,
  interval_year_month(str1) - interval_year_month(str1),
  interval '1-2' year to month - interval_year_month(str1)
from vector_interval_1 order by dt;

explain vectorization expression
select
  dt,
  interval '1 2:3:4' day to second + interval '1 2:3:4' day to second,
  interval_day_time(str2) + interval_day_time(str2),
  interval '1 2:3:4' day to second + interval_day_time(str2),
  interval '1 2:3:4' day to second - interval '1 2:3:4' day to second,
  interval_day_time(str2) - interval_day_time(str2),
  interval '1 2:3:4' day to second - interval_day_time(str2)
from vector_interval_1 order by dt;

select
  dt,
  interval '1 2:3:4' day to second + interval '1 2:3:4' day to second,
  interval_day_time(str2) + interval_day_time(str2),
  interval '1 2:3:4' day to second + interval_day_time(str2),
  interval '1 2:3:4' day to second - interval '1 2:3:4' day to second,
  interval_day_time(str2) - interval_day_time(str2),
  interval '1 2:3:4' day to second - interval_day_time(str2)
from vector_interval_1 order by dt;


-- date-interval arithmetic
explain vectorization expression
select
  dt,
  dt + interval '1-2' year to month,
  dt + interval_year_month(str1),
  interval '1-2' year to month + dt,
  interval_year_month(str1) + dt,
  dt - interval '1-2' year to month,
  dt - interval_year_month(str1),
  dt + interval '1 2:3:4' day to second,
  dt + interval_day_time(str2),
  interval '1 2:3:4' day to second + dt,
  interval_day_time(str2) + dt,
  dt - interval '1 2:3:4' day to second,
  dt - interval_day_time(str2)
from vector_interval_1 order by dt;

select
  dt,
  dt + interval '1-2' year to month,
  dt + interval_year_month(str1),
  interval '1-2' year to month + dt,
  interval_year_month(str1) + dt,
  dt - interval '1-2' year to month,
  dt - interval_year_month(str1),
  dt + interval '1 2:3:4' day to second,
  dt + interval_day_time(str2),
  interval '1 2:3:4' day to second + dt,
  interval_day_time(str2) + dt,
  dt - interval '1 2:3:4' day to second,
  dt - interval_day_time(str2)
from vector_interval_1 order by dt;


-- timestamp-interval arithmetic
explain vectorization expression
select
  ts,
  ts + interval '1-2' year to month,
  ts + interval_year_month(str1),
  interval '1-2' year to month + ts,
  interval_year_month(str1) + ts,
  ts - interval '1-2' year to month,
  ts - interval_year_month(str1),
  ts + interval '1 2:3:4' day to second,
  ts + interval_day_time(str2),
  interval '1 2:3:4' day to second + ts,
  interval_day_time(str2) + ts,
  ts - interval '1 2:3:4' day to second,
  ts - interval_day_time(str2)
from vector_interval_1 order by ts;

select
  ts,
  ts + interval '1-2' year to month,
  ts + interval_year_month(str1),
  interval '1-2' year to month + ts,
  interval_year_month(str1) + ts,
  ts - interval '1-2' year to month,
  ts - interval_year_month(str1),
  ts + interval '1 2:3:4' day to second,
  ts + interval_day_time(str2),
  interval '1 2:3:4' day to second + ts,
  interval_day_time(str2) + ts,
  ts - interval '1 2:3:4' day to second,
  ts - interval_day_time(str2)
from vector_interval_1 order by ts;


-- timestamp-timestamp arithmetic
explain vectorization expression
select
  ts,
  ts - ts,
  timestamp '2001-01-01 01:02:03' - ts,
  ts - timestamp '2001-01-01 01:02:03'
from vector_interval_1 order by ts;

select
  ts,
  ts - ts,
  timestamp '2001-01-01 01:02:03' - ts,
  ts - timestamp '2001-01-01 01:02:03'
from vector_interval_1 order by ts;


-- date-date arithmetic
explain vectorization expression
select
  dt,
  dt - dt,
  date '2001-01-01' - dt,
  dt - date '2001-01-01'
from vector_interval_1 order by dt;

select
  dt,
  dt - dt,
  date '2001-01-01' - dt,
  dt - date '2001-01-01'
from vector_interval_1 order by dt;


-- date-timestamp arithmetic
explain vectorization expression
select
  dt,
  ts - dt,
  timestamp '2001-01-01 01:02:03' - dt,
  ts - date '2001-01-01',
  dt - ts,
  dt - timestamp '2001-01-01 01:02:03',
  date '2001-01-01' - ts
from vector_interval_1 order by dt;

select
  dt,
  ts - dt,
  timestamp '2001-01-01 01:02:03' - dt,
  ts - date '2001-01-01',
  dt - ts,
  dt - timestamp '2001-01-01 01:02:03',
  date '2001-01-01' - ts
from vector_interval_1 order by dt;
