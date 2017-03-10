create table t (a int);

insert into t values (1),(1),(2);

explain select count(*) from t group by ();

select count(*) from t group by ();


