
with
    test1 as (
        with t1 as (select 1)
        select 1
    )
select * from test1;

with
    test2 as (
        with
            test2 as (
                select 1 as a
            )
        select 1
    )
select * from test2;

with
    test3 as (
        with
            t1 as (
                select 1 as a
            ),
            t2 as (
                select 1 as b
            )
        select * from t1 join t2 on a=b
    )
select * from test3;

create table table_1(a int, b int);

with
    test4 as (
        with t1 as (select * from table_1)
        select * from t1
    )
select * from test4;

with
    q1 as (
        with t3 as (select 1)
        select * from t3
    ),
    q2 as (
        with t3 as (select * from q1)
        select * from t3
    )
select * from q2;
