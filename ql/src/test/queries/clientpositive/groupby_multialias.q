create table t1_n150 (a int);

explain
select t1_n150.a as a1, min(t1_n150.a) as a
from t1_n150
group by t1_n150.a;

