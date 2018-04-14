--! qt:dataset:src

-- should all be true
select
  i1 = i1,
  i1 = i2,
  i1 >= i2,
  i1 <= i2,
  i3 = i3,
  i3 = i4,
  i3 <= i4,
  i3 >= i4,
  i1 < i3,
  i3 > i1,
  i1 != i3
from (
  select
    interval '2-0' year to month as i1,
    interval '2' year as i2,
    interval '2-1' year to month as i3,
    interval '25' month as i4
  from src limit 1
) q1;

-- should all be false
select
  i1 != i1,
  i1 != i2,
  i1 < i2,
  i1 > i2,
  i1 = i3,
  i1 > i3,
  i1 >= i3,
  i3 < i1,
  i3 <= i1
from (
  select
    interval '2-0' year to month as i1,
    interval '2' year as i2,
    interval '2-1' year to month as i3,
    interval '25' month as i4
  from src limit 1
) q1;

-- should all be true
select
  i1 = i1,
  i1 = i2,
  i1 >= i2,
  i1 <= i2,
  i3 = i3,
  i3 = i4,
  i3 <= i4,
  i3 >= i4,
  i1 < i3,
  i3 > i1,
  i1 != i3
from (
  select
    interval '1 0:0:0' day to second as i1,
    interval '24' hour as i2,
    interval '1 0:0:1' day to second as i3,
    interval '86401' second as i4
  from src limit 1
) q1;

-- should all be false
select
  i1 != i1,
  i1 != i2,
  i1 < i2,
  i1 > i2,
  i1 = i3,
  i1 > i3,
  i1 >= i3,
  i3 < i1,
  i3 <= i1
from (
  select
    interval '1 0:0:0' day to second as i1,
    interval '24' hour as i2,
    interval '1 0:0:1' day to second as i3,
    interval '86401' second as i4
  from src limit 1
) q1;

