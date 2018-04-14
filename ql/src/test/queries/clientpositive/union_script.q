--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS
select * from (
  select transform(key) using 'cat' as cola from src)s;

select * from (
  select transform(key) using 'cat' as cola from src
  union all
  select transform(key) using 'cat' as cola from src) s;
