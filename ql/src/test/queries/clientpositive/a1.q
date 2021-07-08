create temporary table t (a integer) stored as orc;
insert into t values(1);
insert into t values(1),(2);
insert into t values(1),(2),(3);

create table t2 as select * from t;
