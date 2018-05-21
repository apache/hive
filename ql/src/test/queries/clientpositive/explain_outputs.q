
create table t1_n22 (id int);
create table t2_n14 (id int);

insert into t1_n22 values (1),(10);
insert into t2_n14 values (1),(2),(3),(4),(5);

explain
select sum(t1_n22.id) from t1_n22 join t2_n14 on (t1_n22.id=t2_n14.id);

explain analyze
select sum(t1_n22.id) from t1_n22 join t2_n14 on (t1_n22.id=t2_n14.id);

explain reoptimization
select sum(t1_n22.id) from t1_n22 join t2_n14 on (t1_n22.id=t2_n14.id);
