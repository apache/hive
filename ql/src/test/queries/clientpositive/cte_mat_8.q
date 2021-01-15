set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;
set hive.explain.user=true;

with x as ( select 'x' as id ), -- not materialized
a1 as ( select 'a1' as id ), -- materialized by a2 and the root
a2 as ( select 'a2 <- ' || id as id from a1) -- materialized by the root
select * from a1
union all
select * from x
union all
select * from a2
union all
select * from a2;
