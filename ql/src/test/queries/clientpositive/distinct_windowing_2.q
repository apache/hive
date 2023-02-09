-- Test Order By clause has expressions when query has distinct + window func and select expressions has alias defined
create table t1 (a bigint, b int, c int);
create table t2 (c bigint, d int);

insert into t1(a, b, c) values
(1, 1, 1),
(1, 2, 2),
(2, 2, 2),
(3, 2, 2);

select distinct c as c_alias,
       rank() over (order by b) as rank_alias
from t1
order by c;

select distinct c as c_alias,
       rank() over (order by b) as rank_alias
from t1
order by rank() over (order by b);




explain cbo
select distinct c as c_alias,
       rank() over (order by b) as rank_alias
from t1
order by c;

explain cbo
select distinct b + d as b_plus_d,
       rank() over (order by b) as rank_alias
from t1
join t2 on t1.a = t2.c
order by b + d;
