create table t (a int);
create table t2 (b int);
create table t3 (c int);

insert into t values(3),(10);

explain select a from t,t2,t3 where
 (a>3 and null between 0 and 10) is null
 ;

explain select a from t,t2,t3 where
 (a>5 or null between 0 and 10) and (a*a < 101)
 and t.a=t2.b and t.a=t3.c
 ;
