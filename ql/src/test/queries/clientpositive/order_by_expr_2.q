set hive.fetch.task.conversion=none;

create table t(a int, b int);

insert into t values (1,2),(1,2),(1,3),(2,4),(20,-100),(-1000,100),(4,5),(3,7),(8,9);

select a as b, b as a from t order by a;
select a as b, b as a from t order by t.a;
select a as b from t order by b;
select a as b from t order by 0-a;
select a,b,count(*),a+b from t group by a, b order by a+b;
