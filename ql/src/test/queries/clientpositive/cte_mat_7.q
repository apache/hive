set hive.optimize.cte.materialize.threshold=1;

create table t0(col0 int);

insert into t0(col0) values
(1),(2),
(100),(100),(100),
(200),(200);

explain
create table t1 as
with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, (select small_count from cte)
from t0
order by t0.col0;

create table t1 as
with cte as (select count(*) as small_count from t0 where col0 < 10)
select t0.col0, (select small_count from cte)
from t0
order by t0.col0;

select * from t1;
