create table t1 (a int, b int);

-- distribute by and sort by
explain ast
select * from t1 distribute by a, b sort by a;

explain cbo
select * from t1 distribute by a, b sort by a;

explain
select * from t1 distribute by a, b sort by a;

-- distribute by only
explain ast
select * from t1 distribute by a, b;

explain cbo
select * from t1 distribute by a, b;

explain
select * from t1 distribute by a, b;

-- cluster by
explain ast
select * from t1 cluster by a, b;

explain cbo
select * from t1 cluster by a, b;

explain
select * from t1 cluster by a, b;


-- order by
explain ast
select * from t1 order by a, b;

explain cbo
select * from t1 order by a, b;

explain
select * from t1 order by a, b;
