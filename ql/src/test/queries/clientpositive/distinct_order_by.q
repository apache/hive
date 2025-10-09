create table t1 (a bigint, b int);

insert into t1(a, b) values
(1, 1),
(1, 2),
(2, 2),
(3, 2);

explain cbo
select distinct b + 2 as alias_b, b
from t1
order by b + 2, b;


select distinct b + 2 as alias_b, b
from t1
order by b + 2, b;
