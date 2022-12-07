set hive.cli.print.header=true;

create table t1 (a1 int, b1 int, c1 struct<f1:string, f2:string, f3:string>);
create table t2 (a2 int, b2 int);

explain cbo
select a1 from t1
join (select a2 from t2 where 1 = 0) s on s.a2 = t1.a1;

explain
select a1 from t1
join (select a2 from t2 where 1 = 0) s on s.a2 = t1.a1;

select a1 from t1
join (select a2 from t2 where 1 = 0) s on s.a2 = t1.a1;

explain cbo
select y + 1 from (select a1 y, b1 z from t1 where b1 > 10) q WHERE 1=0;

explain
select y + 1 from (select a1 y, b1 z from t1 where b1 > 10) q WHERE 1=0;

select y + 1 from (select a1 y, b1 z from t1 where b1 > 10) q WHERE 1=0;


create view vw1 as (select t1.b1, t2.b2 from t1, t2 WHERE t1.a1 = t2.a2);

explain cbo
select 1 from vw1 where 1=0;

explain
select 1 from vw1 where 1=0;


explain cbo
select count(a1) from t1 where 1=0 group by a1 order by a1;
explain
select count(a1) from t1 where 1=0 group by a1 order by a1;

explain cbo
select min(c1) from t1 where false;

explain cbo
select b1, count(a1) count1 from (select a1, b1 from t1) s where 1=0 group by b1;
select b1, count(a1) count1 from (select a1, b1 from t1) s where 1=0 group by b1;
