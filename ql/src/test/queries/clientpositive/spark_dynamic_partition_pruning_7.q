--! qt:dataset:src
--! qt:dataset:srcpart
set hive.spark.dynamic.partition.pruning=true;

-- SORT_QUERY_RESULTS

-- This qfile tests MapWorks won't be combined if they're targets of different DPP sinks

-- MapWorks for srcpart shouldn't be combined because they're pruned by different DPP sinks
explain
select * from
  (select srcpart.ds,srcpart.key from srcpart join src on srcpart.ds=src.key) a
union all
  (select srcpart.ds,srcpart.key from srcpart join src on srcpart.ds=src.value);

-- MapWorks for srcpart shouldn't be combined because although they're pruned by the same DPP sink,
-- the target columns are different.
explain
select * from
  (select srcpart.ds,srcpart.hr,srcpart.key from srcpart join src on srcpart.ds=src.key) a
union all
  (select srcpart.ds,srcpart.hr,srcpart.key from srcpart join src on srcpart.hr=src.key);
