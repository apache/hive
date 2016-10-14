set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

drop table if exists vector_interval_2;
create table vector_interval_2 (ts timestamp, dt date, str1 string, str2 string, str3 string, str4 string) stored as orc;

insert into vector_interval_2
  select timestamp '2001-01-01 01:02:03', date '2001-01-01', '1-2', '1-3', '1 2:3:4', '1 2:3:5' from src limit 1;
insert into vector_interval_2
  select null, null, null, null, null, null from src limit 1;


-- interval comparisons in select clause

explain vectorization expression
select
  str1,
  -- Should all be true
  interval_year_month(str1) = interval_year_month(str1),
  interval_year_month(str1) <= interval_year_month(str1),
  interval_year_month(str1) <= interval_year_month(str2),
  interval_year_month(str1) < interval_year_month(str2),
  interval_year_month(str1) >= interval_year_month(str1),
  interval_year_month(str2) >= interval_year_month(str1),
  interval_year_month(str2) > interval_year_month(str1),
  interval_year_month(str1) != interval_year_month(str2),

  interval_year_month(str1) = interval '1-2' year to month,
  interval_year_month(str1) <= interval '1-2' year to month,
  interval_year_month(str1) <= interval '1-3' year to month,
  interval_year_month(str1) < interval '1-3' year to month,
  interval_year_month(str1) >= interval '1-2' year to month,
  interval_year_month(str2) >= interval '1-2' year to month,
  interval_year_month(str2) > interval '1-2' year to month,
  interval_year_month(str1) != interval '1-3' year to month,

  interval '1-2' year to month = interval_year_month(str1),
  interval '1-2' year to month <= interval_year_month(str1),
  interval '1-2' year to month <= interval_year_month(str2),
  interval '1-2' year to month < interval_year_month(str2),
  interval '1-2' year to month >= interval_year_month(str1),
  interval '1-3' year to month >= interval_year_month(str1),
  interval '1-3' year to month > interval_year_month(str1),
  interval '1-2' year to month != interval_year_month(str2)
from vector_interval_2 order by str1;

select
  str1,
  -- Should all be true
  interval_year_month(str1) = interval_year_month(str1),
  interval_year_month(str1) <= interval_year_month(str1),
  interval_year_month(str1) <= interval_year_month(str2),
  interval_year_month(str1) < interval_year_month(str2),
  interval_year_month(str1) >= interval_year_month(str1),
  interval_year_month(str2) >= interval_year_month(str1),
  interval_year_month(str2) > interval_year_month(str1),
  interval_year_month(str1) != interval_year_month(str2),

  interval_year_month(str1) = interval '1-2' year to month,
  interval_year_month(str1) <= interval '1-2' year to month,
  interval_year_month(str1) <= interval '1-3' year to month,
  interval_year_month(str1) < interval '1-3' year to month,
  interval_year_month(str1) >= interval '1-2' year to month,
  interval_year_month(str2) >= interval '1-2' year to month,
  interval_year_month(str2) > interval '1-2' year to month,
  interval_year_month(str1) != interval '1-3' year to month,

  interval '1-2' year to month = interval_year_month(str1),
  interval '1-2' year to month <= interval_year_month(str1),
  interval '1-2' year to month <= interval_year_month(str2),
  interval '1-2' year to month < interval_year_month(str2),
  interval '1-2' year to month >= interval_year_month(str1),
  interval '1-3' year to month >= interval_year_month(str1),
  interval '1-3' year to month > interval_year_month(str1),
  interval '1-2' year to month != interval_year_month(str2)
from vector_interval_2 order by str1;

explain vectorization expression
select
  str1,
  -- Should all be false
  interval_year_month(str1) != interval_year_month(str1),
  interval_year_month(str1) >= interval_year_month(str2),
  interval_year_month(str1) > interval_year_month(str2),
  interval_year_month(str2) <= interval_year_month(str1),
  interval_year_month(str2) < interval_year_month(str1),
  interval_year_month(str1) != interval_year_month(str1),

  interval_year_month(str1) != interval '1-2' year to month,
  interval_year_month(str1) >= interval '1-3' year to month,
  interval_year_month(str1) > interval '1-3' year to month,
  interval_year_month(str2) <= interval '1-2' year to month,
  interval_year_month(str2) < interval '1-2' year to month,
  interval_year_month(str1) != interval '1-2' year to month,

  interval '1-2' year to month != interval_year_month(str1),
  interval '1-2' year to month >= interval_year_month(str2),
  interval '1-2' year to month > interval_year_month(str2),
  interval '1-3' year to month <= interval_year_month(str1),
  interval '1-3' year to month < interval_year_month(str1),
  interval '1-2' year to month != interval_year_month(str1)
from vector_interval_2 order by str1;

select
  str1,
  -- Should all be false
  interval_year_month(str1) != interval_year_month(str1),
  interval_year_month(str1) >= interval_year_month(str2),
  interval_year_month(str1) > interval_year_month(str2),
  interval_year_month(str2) <= interval_year_month(str1),
  interval_year_month(str2) < interval_year_month(str1),
  interval_year_month(str1) != interval_year_month(str1),

  interval_year_month(str1) != interval '1-2' year to month,
  interval_year_month(str1) >= interval '1-3' year to month,
  interval_year_month(str1) > interval '1-3' year to month,
  interval_year_month(str2) <= interval '1-2' year to month,
  interval_year_month(str2) < interval '1-2' year to month,
  interval_year_month(str1) != interval '1-2' year to month,

  interval '1-2' year to month != interval_year_month(str1),
  interval '1-2' year to month >= interval_year_month(str2),
  interval '1-2' year to month > interval_year_month(str2),
  interval '1-3' year to month <= interval_year_month(str1),
  interval '1-3' year to month < interval_year_month(str1),
  interval '1-2' year to month != interval_year_month(str1)
from vector_interval_2 order by str1;

explain vectorization expression
select
  str3,
  -- Should all be true
  interval_day_time(str3) = interval_day_time(str3),
  interval_day_time(str3) <= interval_day_time(str3),
  interval_day_time(str3) <= interval_day_time(str4),
  interval_day_time(str3) < interval_day_time(str4),
  interval_day_time(str3) >= interval_day_time(str3),
  interval_day_time(str4) >= interval_day_time(str3),
  interval_day_time(str4) > interval_day_time(str3),
  interval_day_time(str3) != interval_day_time(str4),

  interval_day_time(str3) = interval '1 2:3:4' day to second,
  interval_day_time(str3) <= interval '1 2:3:4' day to second,
  interval_day_time(str3) <= interval '1 2:3:5' day to second,
  interval_day_time(str3) < interval '1 2:3:5' day to second,
  interval_day_time(str3) >= interval '1 2:3:4' day to second,
  interval_day_time(str4) >= interval '1 2:3:4' day to second,
  interval_day_time(str4) > interval '1 2:3:4' day to second,
  interval_day_time(str3) != interval '1 2:3:5' day to second,

  interval '1 2:3:4' day to second = interval_day_time(str3),
  interval '1 2:3:4' day to second <= interval_day_time(str3),
  interval '1 2:3:4' day to second <= interval_day_time(str4),
  interval '1 2:3:4' day to second < interval_day_time(str4),
  interval '1 2:3:4' day to second >= interval_day_time(str3),
  interval '1 2:3:5' day to second >= interval_day_time(str3),
  interval '1 2:3:5' day to second > interval_day_time(str3),
  interval '1 2:3:4' day to second != interval_day_time(str4)
from vector_interval_2 order by str3;

select
  str3,
  -- Should all be true
  interval_day_time(str3) = interval_day_time(str3),
  interval_day_time(str3) <= interval_day_time(str3),
  interval_day_time(str3) <= interval_day_time(str4),
  interval_day_time(str3) < interval_day_time(str4),
  interval_day_time(str3) >= interval_day_time(str3),
  interval_day_time(str4) >= interval_day_time(str3),
  interval_day_time(str4) > interval_day_time(str3),
  interval_day_time(str3) != interval_day_time(str4),

  interval_day_time(str3) = interval '1 2:3:4' day to second,
  interval_day_time(str3) <= interval '1 2:3:4' day to second,
  interval_day_time(str3) <= interval '1 2:3:5' day to second,
  interval_day_time(str3) < interval '1 2:3:5' day to second,
  interval_day_time(str3) >= interval '1 2:3:4' day to second,
  interval_day_time(str4) >= interval '1 2:3:4' day to second,
  interval_day_time(str4) > interval '1 2:3:4' day to second,
  interval_day_time(str3) != interval '1 2:3:5' day to second,

  interval '1 2:3:4' day to second = interval_day_time(str3),
  interval '1 2:3:4' day to second <= interval_day_time(str3),
  interval '1 2:3:4' day to second <= interval_day_time(str4),
  interval '1 2:3:4' day to second < interval_day_time(str4),
  interval '1 2:3:4' day to second >= interval_day_time(str3),
  interval '1 2:3:5' day to second >= interval_day_time(str3),
  interval '1 2:3:5' day to second > interval_day_time(str3),
  interval '1 2:3:4' day to second != interval_day_time(str4)
from vector_interval_2 order by str3;

explain vectorization expression
select
  str3,
  -- Should all be false
  interval_day_time(str3) != interval_day_time(str3),
  interval_day_time(str3) >= interval_day_time(str4),
  interval_day_time(str3) > interval_day_time(str4),
  interval_day_time(str4) <= interval_day_time(str3),
  interval_day_time(str4) < interval_day_time(str3),
  interval_day_time(str3) != interval_day_time(str3),

  interval_day_time(str3) != interval '1 2:3:4' day to second,
  interval_day_time(str3) >= interval '1 2:3:5' day to second,
  interval_day_time(str3) > interval '1 2:3:5' day to second,
  interval_day_time(str4) <= interval '1 2:3:4' day to second,
  interval_day_time(str4) < interval '1 2:3:4' day to second,
  interval_day_time(str3) != interval '1 2:3:4' day to second,

  interval '1 2:3:4' day to second != interval_day_time(str3),
  interval '1 2:3:4' day to second >= interval_day_time(str4),
  interval '1 2:3:4' day to second > interval_day_time(str4),
  interval '1 2:3:5' day to second <= interval_day_time(str3),
  interval '1 2:3:5' day to second < interval_day_time(str3),
  interval '1 2:3:4' day to second != interval_day_time(str3)
from vector_interval_2 order by str3;

select
  str3,
  -- Should all be false
  interval_day_time(str3) != interval_day_time(str3),
  interval_day_time(str3) >= interval_day_time(str4),
  interval_day_time(str3) > interval_day_time(str4),
  interval_day_time(str4) <= interval_day_time(str3),
  interval_day_time(str4) < interval_day_time(str3),
  interval_day_time(str3) != interval_day_time(str3),

  interval_day_time(str3) != interval '1 2:3:4' day to second,
  interval_day_time(str3) >= interval '1 2:3:5' day to second,
  interval_day_time(str3) > interval '1 2:3:5' day to second,
  interval_day_time(str4) <= interval '1 2:3:4' day to second,
  interval_day_time(str4) < interval '1 2:3:4' day to second,
  interval_day_time(str3) != interval '1 2:3:4' day to second,

  interval '1 2:3:4' day to second != interval_day_time(str3),
  interval '1 2:3:4' day to second >= interval_day_time(str4),
  interval '1 2:3:4' day to second > interval_day_time(str4),
  interval '1 2:3:5' day to second <= interval_day_time(str3),
  interval '1 2:3:5' day to second < interval_day_time(str3),
  interval '1 2:3:4' day to second != interval_day_time(str3)
from vector_interval_2 order by str3;


-- interval expressions in predicates
explain vectorization expression
select ts from vector_interval_2
where
  interval_year_month(str1) = interval_year_month(str1)
  and interval_year_month(str1) != interval_year_month(str2)
  and interval_year_month(str1) <= interval_year_month(str2)
  and interval_year_month(str1) < interval_year_month(str2)
  and interval_year_month(str2) >= interval_year_month(str1)
  and interval_year_month(str2) > interval_year_month(str1)

  and interval_year_month(str1) = interval '1-2' year to month
  and interval_year_month(str1) != interval '1-3' year to month
  and interval_year_month(str1) <= interval '1-3' year to month
  and interval_year_month(str1) < interval '1-3' year to month
  and interval_year_month(str2) >= interval '1-2' year to month
  and interval_year_month(str2) > interval '1-2' year to month

  and interval '1-2' year to month = interval_year_month(str1)
  and interval '1-2' year to month != interval_year_month(str2)
  and interval '1-2' year to month <= interval_year_month(str2)
  and interval '1-2' year to month < interval_year_month(str2)
  and interval '1-3' year to month >= interval_year_month(str1)
  and interval '1-3' year to month > interval_year_month(str1)
order by ts;

select ts from vector_interval_2
where
  interval_year_month(str1) = interval_year_month(str1)
  and interval_year_month(str1) != interval_year_month(str2)
  and interval_year_month(str1) <= interval_year_month(str2)
  and interval_year_month(str1) < interval_year_month(str2)
  and interval_year_month(str2) >= interval_year_month(str1)
  and interval_year_month(str2) > interval_year_month(str1)

  and interval_year_month(str1) = interval '1-2' year to month
  and interval_year_month(str1) != interval '1-3' year to month
  and interval_year_month(str1) <= interval '1-3' year to month
  and interval_year_month(str1) < interval '1-3' year to month
  and interval_year_month(str2) >= interval '1-2' year to month
  and interval_year_month(str2) > interval '1-2' year to month

  and interval '1-2' year to month = interval_year_month(str1)
  and interval '1-2' year to month != interval_year_month(str2)
  and interval '1-2' year to month <= interval_year_month(str2)
  and interval '1-2' year to month < interval_year_month(str2)
  and interval '1-3' year to month >= interval_year_month(str1)
  and interval '1-3' year to month > interval_year_month(str1)
order by ts;

explain vectorization expression
select ts from vector_interval_2
where
  interval_day_time(str3) = interval_day_time(str3)
  and interval_day_time(str3) != interval_day_time(str4)
  and interval_day_time(str3) <= interval_day_time(str4)
  and interval_day_time(str3) < interval_day_time(str4)
  and interval_day_time(str4) >= interval_day_time(str3)
  and interval_day_time(str4) > interval_day_time(str3)

  and interval_day_time(str3) = interval '1 2:3:4' day to second
  and interval_day_time(str3) != interval '1 2:3:5' day to second
  and interval_day_time(str3) <= interval '1 2:3:5' day to second
  and interval_day_time(str3) < interval '1 2:3:5' day to second
  and interval_day_time(str4) >= interval '1 2:3:4' day to second
  and interval_day_time(str4) > interval '1 2:3:4' day to second

  and interval '1 2:3:4' day to second = interval_day_time(str3)
  and interval '1 2:3:4' day to second != interval_day_time(str4)
  and interval '1 2:3:4' day to second <= interval_day_time(str4)
  and interval '1 2:3:4' day to second < interval_day_time(str4)
  and interval '1 2:3:5' day to second >= interval_day_time(str3)
  and interval '1 2:3:5' day to second > interval_day_time(str3)
order by ts;

select ts from vector_interval_2
where
  interval_day_time(str3) = interval_day_time(str3)
  and interval_day_time(str3) != interval_day_time(str4)
  and interval_day_time(str3) <= interval_day_time(str4)
  and interval_day_time(str3) < interval_day_time(str4)
  and interval_day_time(str4) >= interval_day_time(str3)
  and interval_day_time(str4) > interval_day_time(str3)

  and interval_day_time(str3) = interval '1 2:3:4' day to second
  and interval_day_time(str3) != interval '1 2:3:5' day to second
  and interval_day_time(str3) <= interval '1 2:3:5' day to second
  and interval_day_time(str3) < interval '1 2:3:5' day to second
  and interval_day_time(str4) >= interval '1 2:3:4' day to second
  and interval_day_time(str4) > interval '1 2:3:4' day to second

  and interval '1 2:3:4' day to second = interval_day_time(str3)
  and interval '1 2:3:4' day to second != interval_day_time(str4)
  and interval '1 2:3:4' day to second <= interval_day_time(str4)
  and interval '1 2:3:4' day to second < interval_day_time(str4)
  and interval '1 2:3:5' day to second >= interval_day_time(str3)
  and interval '1 2:3:5' day to second > interval_day_time(str3)
order by ts;

explain vectorization expression
select ts from vector_interval_2
where
  date '2002-03-01' = dt + interval_year_month(str1)
  and date '2002-03-01' <= dt + interval_year_month(str1)
  and date '2002-03-01' >= dt + interval_year_month(str1)
  and dt + interval_year_month(str1) = date '2002-03-01'
  and dt + interval_year_month(str1) <= date '2002-03-01'
  and dt + interval_year_month(str1) >= date '2002-03-01'
  and dt != dt + interval_year_month(str1)

  and date '2002-03-01' = dt + interval '1-2' year to month
  and date '2002-03-01' <= dt + interval '1-2' year to month
  and date '2002-03-01' >= dt + interval '1-2' year to month
  and dt + interval '1-2' year to month = date '2002-03-01'
  and dt + interval '1-2' year to month <= date '2002-03-01'
  and dt + interval '1-2' year to month >= date '2002-03-01'
  and dt != dt + interval '1-2' year to month
order by ts;

select ts from vector_interval_2
where
  date '2002-03-01' = dt + interval_year_month(str1)
  and date '2002-03-01' <= dt + interval_year_month(str1)
  and date '2002-03-01' >= dt + interval_year_month(str1)
  and dt + interval_year_month(str1) = date '2002-03-01'
  and dt + interval_year_month(str1) <= date '2002-03-01'
  and dt + interval_year_month(str1) >= date '2002-03-01'
  and dt != dt + interval_year_month(str1)

  and date '2002-03-01' = dt + interval '1-2' year to month
  and date '2002-03-01' <= dt + interval '1-2' year to month
  and date '2002-03-01' >= dt + interval '1-2' year to month
  and dt + interval '1-2' year to month = date '2002-03-01'
  and dt + interval '1-2' year to month <= date '2002-03-01'
  and dt + interval '1-2' year to month >= date '2002-03-01'
  and dt != dt + interval '1-2' year to month
order by ts;

explain vectorization expression
select ts from vector_interval_2
where
  timestamp '2002-03-01 01:02:03' = ts + interval '1-2' year to month
  and timestamp '2002-03-01 01:02:03' <= ts + interval '1-2' year to month
  and timestamp '2002-03-01 01:02:03' >= ts + interval '1-2' year to month
  and timestamp '2002-04-01 01:02:03' != ts + interval '1-2' year to month
  and timestamp '2002-02-01 01:02:03' < ts + interval '1-2' year to month
  and timestamp '2002-04-01 01:02:03' > ts + interval '1-2' year to month

  and ts + interval '1-2' year to month = timestamp '2002-03-01 01:02:03'
  and ts + interval '1-2' year to month >= timestamp '2002-03-01 01:02:03'
  and ts + interval '1-2' year to month <= timestamp '2002-03-01 01:02:03'
  and ts + interval '1-2' year to month != timestamp '2002-04-01 01:02:03'
  and ts + interval '1-2' year to month > timestamp '2002-02-01 01:02:03'
  and ts + interval '1-2' year to month < timestamp '2002-04-01 01:02:03'

  and ts = ts + interval '0' year
  and ts != ts + interval '1' year
  and ts <= ts + interval '1' year
  and ts < ts + interval '1' year
  and ts >= ts - interval '1' year
  and ts > ts - interval '1' year
order by ts;

select ts from vector_interval_2
where
  timestamp '2002-03-01 01:02:03' = ts + interval '1-2' year to month
  and timestamp '2002-03-01 01:02:03' <= ts + interval '1-2' year to month
  and timestamp '2002-03-01 01:02:03' >= ts + interval '1-2' year to month
  and timestamp '2002-04-01 01:02:03' != ts + interval '1-2' year to month
  and timestamp '2002-02-01 01:02:03' < ts + interval '1-2' year to month
  and timestamp '2002-04-01 01:02:03' > ts + interval '1-2' year to month

  and ts + interval '1-2' year to month = timestamp '2002-03-01 01:02:03'
  and ts + interval '1-2' year to month >= timestamp '2002-03-01 01:02:03'
  and ts + interval '1-2' year to month <= timestamp '2002-03-01 01:02:03'
  and ts + interval '1-2' year to month != timestamp '2002-04-01 01:02:03'
  and ts + interval '1-2' year to month > timestamp '2002-02-01 01:02:03'
  and ts + interval '1-2' year to month < timestamp '2002-04-01 01:02:03'

  and ts = ts + interval '0' year
  and ts != ts + interval '1' year
  and ts <= ts + interval '1' year
  and ts < ts + interval '1' year
  and ts >= ts - interval '1' year
  and ts > ts - interval '1' year
order by ts;

-- day to second expressions in predicate
explain vectorization expression
select ts from vector_interval_2
where
  timestamp '2001-01-01 01:02:03' = dt + interval '0 1:2:3' day to second
  and timestamp '2001-01-01 01:02:03' != dt + interval '0 1:2:4' day to second
  and timestamp '2001-01-01 01:02:03' <= dt + interval '0 1:2:3' day to second
  and timestamp '2001-01-01 01:02:03' < dt + interval '0 1:2:4' day to second
  and timestamp '2001-01-01 01:02:03' >= dt - interval '0 1:2:3' day to second
  and timestamp '2001-01-01 01:02:03' > dt - interval '0 1:2:4' day to second

  and dt + interval '0 1:2:3' day to second = timestamp '2001-01-01 01:02:03'
  and dt + interval '0 1:2:4' day to second != timestamp '2001-01-01 01:02:03'
  and dt + interval '0 1:2:3' day to second >= timestamp '2001-01-01 01:02:03'
  and dt + interval '0 1:2:4' day to second > timestamp '2001-01-01 01:02:03'
  and dt - interval '0 1:2:3' day to second <= timestamp '2001-01-01 01:02:03'
  and dt - interval '0 1:2:4' day to second < timestamp '2001-01-01 01:02:03'

  and ts = dt + interval '0 1:2:3' day to second
  and ts != dt + interval '0 1:2:4' day to second
  and ts <= dt + interval '0 1:2:3' day to second
  and ts < dt + interval '0 1:2:4' day to second
  and ts >= dt - interval '0 1:2:3' day to second
  and ts > dt - interval '0 1:2:4' day to second
order by ts;

select ts from vector_interval_2
where
  timestamp '2001-01-01 01:02:03' = dt + interval '0 1:2:3' day to second
  and timestamp '2001-01-01 01:02:03' != dt + interval '0 1:2:4' day to second
  and timestamp '2001-01-01 01:02:03' <= dt + interval '0 1:2:3' day to second
  and timestamp '2001-01-01 01:02:03' < dt + interval '0 1:2:4' day to second
  and timestamp '2001-01-01 01:02:03' >= dt - interval '0 1:2:3' day to second
  and timestamp '2001-01-01 01:02:03' > dt - interval '0 1:2:4' day to second

  and dt + interval '0 1:2:3' day to second = timestamp '2001-01-01 01:02:03'
  and dt + interval '0 1:2:4' day to second != timestamp '2001-01-01 01:02:03'
  and dt + interval '0 1:2:3' day to second >= timestamp '2001-01-01 01:02:03'
  and dt + interval '0 1:2:4' day to second > timestamp '2001-01-01 01:02:03'
  and dt - interval '0 1:2:3' day to second <= timestamp '2001-01-01 01:02:03'
  and dt - interval '0 1:2:4' day to second < timestamp '2001-01-01 01:02:03'

  and ts = dt + interval '0 1:2:3' day to second
  and ts != dt + interval '0 1:2:4' day to second
  and ts <= dt + interval '0 1:2:3' day to second
  and ts < dt + interval '0 1:2:4' day to second
  and ts >= dt - interval '0 1:2:3' day to second
  and ts > dt - interval '0 1:2:4' day to second
order by ts;

explain vectorization expression
select ts from vector_interval_2
where
  timestamp '2001-01-01 01:02:03' = ts + interval '0' day
  and timestamp '2001-01-01 01:02:03' != ts + interval '1' day
  and timestamp '2001-01-01 01:02:03' <= ts + interval '1' day
  and timestamp '2001-01-01 01:02:03' < ts + interval '1' day
  and timestamp '2001-01-01 01:02:03' >= ts - interval '1' day
  and timestamp '2001-01-01 01:02:03' > ts - interval '1' day

  and ts + interval '0' day = timestamp '2001-01-01 01:02:03'
  and ts + interval '1' day != timestamp '2001-01-01 01:02:03'
  and ts + interval '1' day >= timestamp '2001-01-01 01:02:03'
  and ts + interval '1' day > timestamp '2001-01-01 01:02:03'
  and ts - interval '1' day <= timestamp '2001-01-01 01:02:03'
  and ts - interval '1' day < timestamp '2001-01-01 01:02:03'

  and ts = ts + interval '0' day
  and ts != ts + interval '1' day
  and ts <= ts + interval '1' day
  and ts < ts + interval '1' day
  and ts >= ts - interval '1' day
  and ts > ts - interval '1' day
order by ts;

select ts from vector_interval_2
where
  timestamp '2001-01-01 01:02:03' = ts + interval '0' day
  and timestamp '2001-01-01 01:02:03' != ts + interval '1' day
  and timestamp '2001-01-01 01:02:03' <= ts + interval '1' day
  and timestamp '2001-01-01 01:02:03' < ts + interval '1' day
  and timestamp '2001-01-01 01:02:03' >= ts - interval '1' day
  and timestamp '2001-01-01 01:02:03' > ts - interval '1' day

  and ts + interval '0' day = timestamp '2001-01-01 01:02:03'
  and ts + interval '1' day != timestamp '2001-01-01 01:02:03'
  and ts + interval '1' day >= timestamp '2001-01-01 01:02:03'
  and ts + interval '1' day > timestamp '2001-01-01 01:02:03'
  and ts - interval '1' day <= timestamp '2001-01-01 01:02:03'
  and ts - interval '1' day < timestamp '2001-01-01 01:02:03'

  and ts = ts + interval '0' day
  and ts != ts + interval '1' day
  and ts <= ts + interval '1' day
  and ts < ts + interval '1' day
  and ts >= ts - interval '1' day
  and ts > ts - interval '1' day
order by ts;

drop table vector_interval_2;
