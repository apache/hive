drop table t_one_col;
create table t_one_col (c1 int);

explain
insert into t_one_col(c1) select c1 from t_one_col;
