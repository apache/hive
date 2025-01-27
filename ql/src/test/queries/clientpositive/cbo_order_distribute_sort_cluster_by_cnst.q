-- When orderby.position.alias is disabled, we expect no operation to occur if a constant integer is specified as a key.
set hive.orderby.position.alias=false;

create table t1 (a string, b int, c int);

-- order by
explain cbo
select * from t1 order by 2, 3;
explain
select * from t1 order by 2, 3;

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
select * from t1 cluster by 1;

explain
select * from t1 cluster by 1;

explain cbo
select * from t1 cluster by 1, b;

explain
select * from t1 cluster by 1, b;
