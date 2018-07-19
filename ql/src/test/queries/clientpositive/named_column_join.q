set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table t_n8 (a int, b int);

insert into t_n8 values (1,2),(2,1),(3,4),(4,3),(3,3),(null,null),(null,1),(2,null);

explain select * from t_n8 t1 join t_n8 t2 using (a);

select * from t_n8 t1 join t_n8 t2 using (a);

select * from t_n8 t1 join t_n8 t2 using (a,b);

select t1.a,t2.b,t1.b,t2.a from t_n8 t1 join t_n8 t2 using (a);

select * from t_n8 t1 left outer join t_n8 t2 using (a,b);

select t1.a,t1.b from t_n8 t1 right outer join t_n8 t2 on (t1.a=t2.a and t1.b=t2.b);

select * from t_n8 t1 right outer join t_n8 t2 using (a,b);

select * from t_n8 t1 inner join t_n8 t2 using (a,b);

select * from t_n8 t1 left outer join t_n8 t2 using (b);

select * from t_n8 t1 right outer join t_n8 t2 using (b);

select * from t_n8 t1 inner join t_n8 t2 using (b);

drop view v_n4;

create view v_n4 as select * from t_n8 t1 join t_n8 t2 using (a,b);

desc formatted v_n4;

select * from v_n4;

drop view v_n4;

create view v_n4 as select * from t_n8 t1 right outer join t_n8 t2 using (b,a);

desc formatted v_n4;

select * from v_n4;

select * from (select t1.b b from t_n8 t1 inner join t_n8 t2 using (b)) t3 join t_n8 t4 using(b);

select * from (select t2.a a from t_n8 t1 inner join t_n8 t2 using (b)) t3 join t_n8 t4 using(a);

create table tt_n0 as select * from (select t2.a a from t_n8 t1 inner join t_n8 t2 using (b)) t3 join t_n8 t4 using(a);

desc formatted tt_n0;
