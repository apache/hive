set hive.cli.print.header=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

create table unique_timestamps (tsval timestamp) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/timestamps.txt' OVERWRITE INTO TABLE unique_timestamps;

create table interval_arithmetic_1 (dateval date, tsval timestamp) stored as orc;
insert overwrite table interval_arithmetic_1
  select cast(tsval as date), tsval from unique_timestamps;

SET hive.vectorized.execution.enabled=true;

-- interval year-month arithmetic
explain
select
  dateval,
  dateval - interval '2-2' year to month,
  dateval - interval '-2-2' year to month,
  dateval + interval '2-2' year to month,
  dateval + interval '-2-2' year to month,
  - interval '2-2' year to month + dateval,
  interval '2-2' year to month + dateval
from interval_arithmetic_1
order by dateval;

select
  dateval,
  dateval - interval '2-2' year to month,
  dateval - interval '-2-2' year to month,
  dateval + interval '2-2' year to month,
  dateval + interval '-2-2' year to month,
  - interval '2-2' year to month + dateval,
  interval '2-2' year to month + dateval
from interval_arithmetic_1
order by dateval;

explain
select
  dateval,
  dateval - date '1999-06-07',
  date '1999-06-07' - dateval,
  dateval - dateval
from interval_arithmetic_1
order by dateval;

select
  dateval,
  dateval - date '1999-06-07',
  date '1999-06-07' - dateval,
  dateval - dateval
from interval_arithmetic_1
order by dateval;

explain
select
  tsval,
  tsval - interval '2-2' year to month,
  tsval - interval '-2-2' year to month,
  tsval + interval '2-2' year to month,
  tsval + interval '-2-2' year to month,
  - interval '2-2' year to month + tsval,
  interval '2-2' year to month + tsval
from interval_arithmetic_1
order by tsval;

select
  tsval,
  tsval - interval '2-2' year to month,
  tsval - interval '-2-2' year to month,
  tsval + interval '2-2' year to month,
  tsval + interval '-2-2' year to month,
  - interval '2-2' year to month + tsval,
  interval '2-2' year to month + tsval
from interval_arithmetic_1
order by tsval;

explain
select
  interval '2-2' year to month + interval '3-3' year to month,
  interval '2-2' year to month - interval '3-3' year to month
from interval_arithmetic_1
order by interval '2-2' year to month + interval '3-3' year to month
limit 2;

select
  interval '2-2' year to month + interval '3-3' year to month,
  interval '2-2' year to month - interval '3-3' year to month
from interval_arithmetic_1
order by interval '2-2' year to month + interval '3-3' year to month
limit 2;


-- interval day-time arithmetic
explain
select
  dateval,
  dateval - interval '99 11:22:33.123456789' day to second,
  dateval - interval '-99 11:22:33.123456789' day to second,
  dateval + interval '99 11:22:33.123456789' day to second,
  dateval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + dateval,
  interval '99 11:22:33.123456789' day to second + dateval
from interval_arithmetic_1
order by dateval;

select
  dateval,
  dateval - interval '99 11:22:33.123456789' day to second,
  dateval - interval '-99 11:22:33.123456789' day to second,
  dateval + interval '99 11:22:33.123456789' day to second,
  dateval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + dateval,
  interval '99 11:22:33.123456789' day to second + dateval
from interval_arithmetic_1
order by dateval;

explain
select
  dateval,
  tsval,
  dateval - tsval,
  tsval - dateval,
  tsval - tsval
from interval_arithmetic_1
order by dateval;

select
  dateval,
  tsval,
  dateval - tsval,
  tsval - dateval,
  tsval - tsval
from interval_arithmetic_1
order by dateval;

explain
select
  tsval,
  tsval - interval '99 11:22:33.123456789' day to second,
  tsval - interval '-99 11:22:33.123456789' day to second,
  tsval + interval '99 11:22:33.123456789' day to second,
  tsval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + tsval,
  interval '99 11:22:33.123456789' day to second + tsval
from interval_arithmetic_1
order by tsval;

select
  tsval,
  tsval - interval '99 11:22:33.123456789' day to second,
  tsval - interval '-99 11:22:33.123456789' day to second,
  tsval + interval '99 11:22:33.123456789' day to second,
  tsval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + tsval,
  interval '99 11:22:33.123456789' day to second + tsval
from interval_arithmetic_1
order by tsval;

explain
select
  interval '99 11:22:33.123456789' day to second + interval '10 9:8:7.123456789' day to second,
  interval '99 11:22:33.123456789' day to second - interval '10 9:8:7.123456789' day to second
from interval_arithmetic_1
limit 2;

select
  interval '99 11:22:33.123456789' day to second + interval '10 9:8:7.123456789' day to second,
  interval '99 11:22:33.123456789' day to second - interval '10 9:8:7.123456789' day to second
from interval_arithmetic_1
limit 2;

drop table interval_arithmetic_1;
