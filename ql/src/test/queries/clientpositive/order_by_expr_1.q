set hive.fetch.task.conversion=none;

create table t_n5(a int, b int);

insert into t_n5 values (1,2),(1,2),(1,3),(2,4),(20,-100),(-1000,100),(4,5),(3,7),(8,9);

select a, count(a) from t_n5 group by a order by count(a), a;

explain
select
  interval '2-2' year to month + interval '3-3' year to month,
  interval '2-2' year to month - interval '3-3' year to month
from t_n5 
order by interval '2-2' year to month + interval '3-3' year to month
limit 2;

select a,b, count(*) from t_n5 group by a, b order by a+b;
select a,b, count(*) from t_n5 group by a, b order by count(*), b desc; 
select a,b,count(*),a+b from t_n5 group by a, b order by a+b;
select a,b from t_n5 order by a+b;
select a,b,a+b from t_n5 order by a+b;
select a,b,a+b from t_n5 order by a+b desc;
select cast(0.99999999999999999999 as decimal(20,19)) as c from t_n5 limit 1;
select cast(0.99999999999999999999 as decimal(20,19)) as c from t_n5 order by c limit 1;
select a from t_n5 order by b;
select a from t_n5 order by 0-b;
select b from t_n5 order by 0-b;
select b from t_n5 order by a, 0-b;
select b from t_n5 order by a+1, 0-b;
select b from t_n5 order by 0-b, a+1;
explain select b from t_n5 order by 0-b, a+1;
select a,b from t_n5 order by 0-b;
select a,b from t_n5 order by a, a+1, 0-b;
select a,b from t_n5 order by 0-b, a+1;
select a+1,b from t_n5 order by a, a+1, 0-b;
select a+1 as c, b from t_n5 order by a, a+1, 0-b;
select a, a+1 as c, b from t_n5 order by a, a+1, 0-b;
select a, a+1 as c, b, 2*b from t_n5 order by a, a+1, 0-b;
explain select a, a+1 as c, b, 2*b from t_n5 order by a, a+1, 0-b;
select a, a+1 as c, b, 2*b from t_n5 order by a+1, 0-b;
select a,b, count(*) as c from t_n5 group by a, b order by c, a+b desc; 

select a, max(b) from t_n5 group by a order by count(b), a desc; 
select a, max(b) from t_n5 group by a order by count(b), a; 
