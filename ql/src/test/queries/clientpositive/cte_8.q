set hive.cli.print.header=true;

create table t1(int_col int, bigint_col bigint);

insert into t1 values(1, 2), (3, 4);

explain cbo
with cte1(a, b) as (select int_col x, bigint_col y from t1)
select a, b from cte1;

with cte1(a, b) as (select int_col x, bigint_col y from t1)
select a, b from cte1;

with cte1(a) as (select int_col x, bigint_col y from t1)
select a, y from cte1;

with cte1 as (select int_col x, bigint_col y from t1)
select x, y from cte1;

with cte1 as (select int_col, bigint_col from t1)
select * from cte1;

with cte(c1, c2) as (select int_col, bigint_col y from t1)
select * from cte limit 1;

with cte1(c1, c2) as (select int_col x, sum(bigint_col) y from t1 group by int_col)
select * from cte1;

with cte1(c1, c2) as (
    select int_col x, bigint_col y from t1 where int_col = 1
    union all
    select int_col x, bigint_col y from t1 where int_col = 2
)
select * from cte1;

with cte1(a) as (select int_col x, bigint_col a from t1)
select * from cte1;

with cte1(a) as (select int_col x, bigint_col a from t1)
select cte1.* from cte1;
