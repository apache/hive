select
  interval '10-11' year to month,
  interval '10' year,
  interval '11' month
from src limit 1;

select
  interval_year_month('10-11'),
  interval_year_month(cast('10-11' as string)),
  interval_year_month(cast('10-11' as varchar(10))),
  interval_year_month(cast('10-11' as char(10))),
  interval_year_month('10-11') = interval '10-11' year to month
from src limit 1;

-- Test normalization of interval values
select
  interval '49' month
from src limit 1;

select
  interval '10 9:8:7.987654321' day to second,
  interval '10' day,
  interval '11' hour,
  interval '12' minute,
  interval '13' second,
  interval '13.123456789' second
from src limit 1;

select
  interval_day_time('2 1:2:3'),
  interval_day_time(cast('2 1:2:3' as string)),
  interval_day_time(cast('2 1:2:3' as varchar(10))),
  interval_day_time(cast('2 1:2:3' as char(10))),
  interval_day_time('2 1:2:3') = interval '2 1:2:3' day to second
from src limit 1;

-- Test normalization of interval values
select
  interval '49' hour,
  interval '1470' minute,
  interval '90061.111111111' second
from src limit 1;
