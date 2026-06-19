create table t_n34 (a int);

insert into t_n34 values (1),(1),(2);

explain select count(*) from t_n34 group by ();

select count(*) from t_n34 group by ();


