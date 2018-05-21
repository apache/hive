create table mytable1 (
  mytime timestamp,
  string1 string);

create table t1_n44 as
select
  sum(bound3) OVER (PARTITION BY string1 ORDER BY mytime) as bound1
from (
  select
  string1, mytime,
  lag(mytime) over (partition by string1 order by mytime) as bound3
  from mytable1
) sub;
