set hive.cbo.enable=false;

create table t (i int,a string,b string);

insert into t values (1,'hello','world'),(2,'bye',null);

select * from t where t.b is null;

with cte as (select t.a as a,t.a as b,t.a as c from t where t.b is null) select * from cte;

select t.a as a,t.a as b,t.a as c from t where t.b is null;

with cte as (select t.a as a,t.a as b,t.a as c from t where t.b is not null) select * from cte;

