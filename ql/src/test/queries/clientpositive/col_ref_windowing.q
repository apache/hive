create table t1 (a bigint, b int);

insert into t1(a, b) values (1, 1), (1, 1), (1, NULL), (2, 2);

-- order by window func
select a as c1, rank() over(order by b) c2
from t1
order by a;

-- gby, order by and window func
select a as c1, count(*) c2, rank() over(order by count(*)) c3
from t1
group by a
order by a;
