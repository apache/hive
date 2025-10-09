set hive.optimize.cte.materialize.threshold=2;
set hive.optimize.cte.materialize.full.aggregate.only=false;

explain with x as ( select 'x' as id ),
a1 as ( select 'a1' as id),
a2 as ( select 'a2 <- ' || id as id from a1 ),
a3 as ( select 'a3 <- ' || id as id from a2 ),
b1 as ( select 'b1' as id ),
b2 as (
  select 'b2 <- ' || id as id from b1
  union all
  select 'b2 <- ' || id as id from b1
)
select * from a2
union all
select * from x
union all
select * from a3
union all
select * from a3
union all
select * from b2;

with x as ( select 'x' as id ),
a1 as ( select 'a1' as id),
a2 as ( select 'a2 <- ' || id as id from a1 ),
a3 as ( select 'a3 <- ' || id as id from a2 ),
b1 as ( select 'b1' as id ),
b2 as (
  select 'b2 <- ' || id as id from b1
  union all
  select 'b2 <- ' || id as id from b1
)
select * from a2
union all
select * from x
union all
select * from a3
union all
select * from a3
union all
select * from b2;
