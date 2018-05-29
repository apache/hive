--! qt:dataset:alltypesorc
create table interval_arithmetic_1_n0 (dateval date, tsval timestamp);
insert overwrite table interval_arithmetic_1_n0
  select cast(ctimestamp1 as date), ctimestamp1 from alltypesorc;

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
from interval_arithmetic_1_n0
limit 2;

select
  dateval,
  dateval - interval '2-2' year to month,
  dateval - interval '-2-2' year to month,
  dateval + interval '2-2' year to month,
  dateval + interval '-2-2' year to month,
  - interval '2-2' year to month + dateval,
  interval '2-2' year to month + dateval
from interval_arithmetic_1_n0
limit 2;

explain
select
  dateval,
  dateval - date '1999-06-07',
  date '1999-06-07' - dateval,
  dateval - dateval
from interval_arithmetic_1_n0
limit 2;

select
  dateval,
  dateval - date '1999-06-07',
  date '1999-06-07' - dateval,
  dateval - dateval
from interval_arithmetic_1_n0
limit 2;

explain
select
  tsval,
  tsval - interval '2-2' year to month,
  tsval - interval '-2-2' year to month,
  tsval + interval '2-2' year to month,
  tsval + interval '-2-2' year to month,
  - interval '2-2' year to month + tsval,
  interval '2-2' year to month + tsval
from interval_arithmetic_1_n0
limit 2;

select
  tsval,
  tsval - interval '2-2' year to month,
  tsval - interval '-2-2' year to month,
  tsval + interval '2-2' year to month,
  tsval + interval '-2-2' year to month,
  - interval '2-2' year to month + tsval,
  interval '2-2' year to month + tsval
from interval_arithmetic_1_n0
limit 2;

explain
select
  interval '2-2' year to month + interval '3-3' year to month,
  interval '2-2' year to month - interval '3-3' year to month
from interval_arithmetic_1_n0
limit 2;

select
  interval '2-2' year to month + interval '3-3' year to month,
  interval '2-2' year to month - interval '3-3' year to month
from interval_arithmetic_1_n0
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
from interval_arithmetic_1_n0
limit 2;

select
  dateval,
  dateval - interval '99 11:22:33.123456789' day to second,
  dateval - interval '-99 11:22:33.123456789' day to second,
  dateval + interval '99 11:22:33.123456789' day to second,
  dateval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + dateval,
  interval '99 11:22:33.123456789' day to second + dateval
from interval_arithmetic_1_n0
limit 2;

explain
select
  dateval,
  tsval,
  dateval - tsval,
  tsval - dateval,
  tsval - tsval
from interval_arithmetic_1_n0
limit 2;

select
  dateval,
  tsval,
  dateval - tsval,
  tsval - dateval,
  tsval - tsval
from interval_arithmetic_1_n0
limit 2;

explain
select
  tsval,
  tsval - interval '99 11:22:33.123456789' day to second,
  tsval - interval '-99 11:22:33.123456789' day to second,
  tsval + interval '99 11:22:33.123456789' day to second,
  tsval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + tsval,
  interval '99 11:22:33.123456789' day to second + tsval
from interval_arithmetic_1_n0
limit 2;

select
  tsval,
  tsval - interval '99 11:22:33.123456789' day to second,
  tsval - interval '-99 11:22:33.123456789' day to second,
  tsval + interval '99 11:22:33.123456789' day to second,
  tsval + interval '-99 11:22:33.123456789' day to second,
  -interval '99 11:22:33.123456789' day to second + tsval,
  interval '99 11:22:33.123456789' day to second + tsval
from interval_arithmetic_1_n0
limit 2;

explain
select
  interval '99 11:22:33.123456789' day to second + interval '10 9:8:7.123456789' day to second,
  interval '99 11:22:33.123456789' day to second - interval '10 9:8:7.123456789' day to second
from interval_arithmetic_1_n0
limit 2;

select
  interval '99 11:22:33.123456789' day to second + interval '10 9:8:7.123456789' day to second,
  interval '99 11:22:33.123456789' day to second - interval '10 9:8:7.123456789' day to second
from interval_arithmetic_1_n0
limit 2;

explain
select date '2016-11-08' + interval '1 2:02:00' day to second + interval '2' day + interval '1' hour + interval '1' minute + interval '60' second from interval_arithmetic_1_n0 limit 1;
select date '2016-11-08' + interval '1 2:02:00' day to second + interval '2' day + interval '1' hour + interval '1' minute + interval '60' second from interval_arithmetic_1_n0 limit 1;
drop table interval_arithmetic_1_n0;
