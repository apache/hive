create table t1(int_col int, bigint_col bigint);

explain cbo
with cte1(a) as (select int_col, bigint_col a from t1)
select a from cte1;
