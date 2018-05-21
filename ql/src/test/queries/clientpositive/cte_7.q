set hive.cbo.enable=false;

create table t_n1 (i int,a string,b string);

insert into t_n1 values (1,'hello','world'),(2,'bye',null);

select * from t_n1 where t_n1.b is null;

with cte as (select t_n1.a as a,t_n1.a as b,t_n1.a as c from t_n1 where t_n1.b is null) select * from cte;

select t_n1.a as a,t_n1.a as b,t_n1.a as c from t_n1 where t_n1.b is null;

with cte as (select t_n1.a as a,t_n1.a as b,t_n1.a as c from t_n1 where t_n1.b is not null) select * from cte;

