
with
    test1 as (
        with
            t1 as (
                select 1 as a
            )
        select 1 as b
    )
select * from test1 join t1 on a=b;
