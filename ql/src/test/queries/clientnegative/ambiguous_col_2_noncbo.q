set hive.cbo.enable=false;
create table t1nc (c1 int);
explain select t.c1 from (select t11.c1, t12.c1 from t1nc as t11 inner join t1nc as t12 on t11.c1 = t12.c1) as t;
