set hive.cbo.enable=true;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;
-- SORT_QUERY_RESULTS

with t1 as
  (select stack(3, 1, 1, 2) as (k)),
 t2 as
  (select stack(4, 1, 1, 3, 4) as (k))
select count(*)
from (select distinct k from t1) a
left join t2 b on a.k = b.k;

with t1 as
  (select stack(3, 1, 1, 2) as (k)),
 t2 as
  (select stack(4, 1, 1, 3, 4) as (k))
select b.k
from (select distinct k from t1) a
left join t2 b on a.k = b.k;
