CREATE TABLE t1(i int, j int);
insert into t1 values(1,2);

alter table t1 change j j int constraint nn0 not null enforced;
insert into t1 values(2,null);
