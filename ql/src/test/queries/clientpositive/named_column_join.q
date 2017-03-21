set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table t (a int, b int);

insert into t values (1,2),(2,1),(3,4),(4,3),(3,3),(null,null),(null,1),(2,null);

explain select * from t t1 join t t2 using (a);

select * from t t1 join t t2 using (a);

select * from t t1 join t t2 using (a,b);

select t1.a,t2.b,t1.b,t2.a from t t1 join t t2 using (a);

select * from t t1 left outer join t t2 using (a,b);

select t1.a,t1.b from t t1 right outer join t t2 on (t1.a=t2.a and t1.b=t2.b);

select * from t t1 right outer join t t2 using (a,b);

select * from t t1 inner join t t2 using (a,b);

select * from t t1 left outer join t t2 using (b);

select * from t t1 right outer join t t2 using (b);

select * from t t1 inner join t t2 using (b);

drop view v;

create view v as select * from t t1 join t t2 using (a,b);

desc formatted v;

select * from v;

drop view v;

create view v as select * from t t1 right outer join t t2 using (b,a);

desc formatted v;

select * from v;

select * from (select t1.b b from t t1 inner join t t2 using (b)) t3 join t t4 using(b);

select * from (select t2.a a from t t1 inner join t t2 using (b)) t3 join t t4 using(a);

create table tt as select * from (select t2.a a from t t1 inner join t t2 using (b)) t3 join t t4 using(a);

desc formatted tt;
