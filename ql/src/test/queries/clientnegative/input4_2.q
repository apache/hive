--! qt:dataset:srcpart
set hive.strict.checks.bucketing=false; 

reset hive.mapred.mode;
set hive.strict.checks.cartesian.product=true;

select * from srcpart a join
  (select b.key, count(1) as count from srcpart b where b.ds = '2008-04-08' and b.hr = '14' group by b.key) subq
  where a.ds = '2008-04-08' and a.hr = '11' limit 10;
