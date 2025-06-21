create table t1 (a int);

explain cbo
select
  a * 2 as mul1,
  a * 2 as mul2,
  row_number() over (order by a * 2)
from t1
group by a * 2, a * 2;

explain
select
  a * 2 as mul1,
  a * 2 as mul2,
  row_number() over (order by a * 2)
from t1
group by a * 2, a * 2;
