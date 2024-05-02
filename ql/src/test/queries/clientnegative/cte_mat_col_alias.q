create table t1(int_col int, bigint_col bigint);

set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;

explain cbo
with cte1(a, b, c) as (select int_col x, bigint_col y from t1)
select a, b from cte1;
