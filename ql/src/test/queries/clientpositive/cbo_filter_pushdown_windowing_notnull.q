-- HIVE-29729: an IS NOT NULL predicate above nested windowing (OVER) Projects must
-- not trip the RedundancyChecker in HiveFilterProjectTransposeRule.
set hive.cbo.fallback.strategy=NEVER;

create table tab1 (id string);

select rnk2
from (
  select
    row_number() over(order by rnk) as rnk2
  from (
    select count(*) over() as rnk
    from (select count(*) from tab1) t
  ) t2
) t3
where rnk2 is not null;
