create table t0 (s string);
create table t1 (s string, i int);
insert into t0 select "abc";
insert into t1 select "abc", 4;
select substr(t0.s, t1.i-1) from t0 join t1 on t0.s=t1.s;
