create table t1 (a string, b int);

-- distribute by
explain cbo
select * from t1 distribute by a;
explain
select * from t1 distribute by a;

-- distribute by and sort by
explain cbo
select * from t1 distribute by a, b sort by a;

explain
select * from t1 distribute by a, b sort by a;

-- cluster by
explain cbo
select * from t1 cluster by a, b;

explain
select * from t1 cluster by a, b;
