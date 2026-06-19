set hive.optimize.cte.materialize.threshold=1;

create table t0(col0 int);

insert into t0(col0) values
(1),(2),
(100),(100),(100),
(200),(200);

-- CTE is referenced from scalar subquery in the select clause
explain
with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, (select small_count from cte)
from t0
order by t0.col0;

with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, (select small_count from cte)
from t0
order by t0.col0;

-- disable cte materialization
set hive.optimize.cte.materialize.threshold=-1;

explain
with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, (select small_count from cte)
from t0
order by t0.col0;


with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, (select small_count from cte)
from t0
order by t0.col0;


-- enable cte materialization
set hive.optimize.cte.materialize.threshold=1;

-- CTE is referenced from scalar subquery in the where clause
explain
with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0
from t0
where t0.col0 > (select small_count from cte)
order by t0.col0;

with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0
from t0
where t0.col0 > (select small_count from cte)
order by t0.col0;

-- CTE is referenced from scalar subquery in the having clause
explain
with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, count(*)
from t0
group by col0
having count(*) > (select small_count from cte)
order by t0.col0;

with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, count(*)
from t0
group by col0
having count(*) > (select small_count from cte)
order by t0.col0;

-- mix full aggregate and non-full aggregate ctes
explain
with cte1 as (select col0 as k1 from t0 where col0 = '5'),
     cte2 as (select count(*) as all_count from t0),
     cte3 as (select col0 as k3, col0 + col0 as k3_2x, count(*) as key_count from t0 group by col0)
select t0.col0, count(*)
from t0
join cte1 on t0.col0 = cte1.k1
join cte3 on t0.col0 = cte3.k3
group by col0
having count(*) > (select all_count from cte2)
