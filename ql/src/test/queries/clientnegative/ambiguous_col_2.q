create table t1(c1 int);
explain select t.c1 from (select t11.c1, t12.c1 from t1 as t11 inner join t1 as t12 on t11.c1=t12.c1) as t;

