
with
    test1 as (
        with
            t1 as (
                select 1 as a
            ),
            t1 as (
                select 1 as b
            )
        select 1
    )
select * from test1;
