create table t1 (a int);

explain
select t1.a as a1, min(t1.a) as a
from t1
group by t1.a;

