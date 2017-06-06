set hive.fetch.task.conversion=none;

create table t(a int, b int);

insert into t values (1,2),(1,2),(1,3),(2,4),(20,-100),(-1000,100),(4,5),(3,7),(8,9);

select * from t order by 2;

select * from t order by 1;

select * from t union select * from t order by 1, 2;

select * from t union select * from t order by 2;

select * from t union select * from t order by 1;

select * from (select a, count(a) from t group by a)subq order by 2, 1;

select * from (select a,b, count(*) from t group by a, b)subq order by 3, 2 desc;
 
