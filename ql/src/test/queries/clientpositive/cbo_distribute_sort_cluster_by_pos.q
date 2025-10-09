create table t1 (a string, b int, c int);

-- distribute by
explain cbo
select * from t1 distribute by 2;
explain
select * from t1 distribute by 2;

-- distribute by and sort by
explain cbo
select * from t1 distribute by 1, b sort by 2;

explain
select * from t1 distribute by 1, b sort by 2;

-- cluster by
explain cbo
select * from t1 cluster by 1, b;

explain
select * from t1 cluster by 1, b;
