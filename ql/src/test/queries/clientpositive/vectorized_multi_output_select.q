set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=3000;
set hive.strict.checks.cartesian.product=false;
set hive.merge.nway.joins=false;
set hive.vectorized.execution.enabled=true;

explain
select * from (
  select count(*) as h8_30_to_9
  from src
  join src1 on src.key = src1.key
  where src1.value = "val_278") s1
join (
  select count(*) as h9_to_9_30
  from src
  join src1 on src.key = src1.key
  where src1.value = "val_255") s2;

select * from (
  select count(*) as h8_30_to_9
  from src
  join src1 on src.key = src1.key
  where src1.value = "val_278") s1
join (
  select count(*) as h9_to_9_30
  from src
  join src1 on src.key = src1.key
  where src1.value = "val_255") s2;
