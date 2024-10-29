set hive.tez.cartesian-product.enabled=true;

create table c (a1 int) stored as orc;
create table tmp1 (a int) stored as orc;
create table tmp2 (a int) stored as orc;

insert into table c values (3);
insert into table tmp1 values (3);

explain cbo
with
first as (
select a1 from c where a1 = 3
),
second as (
select a from tmp1
union all
select a from tmp2
)
select a from second cross join first;

explain cbo 
select * from c join tmp1 join tmp2
where c.a1 = tmp1.a and tmp1.a = tmp2.a;
