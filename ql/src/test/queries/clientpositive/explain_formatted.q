
create table t1 (a int, b int);

explain cbo
select a, b from t1 where a = 2 and b in (3, 4, 5);

explain formatted ast
select a, b from t1 where a = 2 and b in (3, 4, 5);

explain formatted
select a, b from t1 where a = 2 and b in (3, 4, 5);

explain cbo
select a in (3, 4, 5) from t1;

explain formatted ast
select a in (3, 4, 5) from t1;

explain formatted
select a in (3, 4, 5) from t1;

explain cbo
select a in (3, 4, 5), a in (1, 2) from t1 where b in (6, 7);

explain formatted ast
select a in (3, 4, 5), a in (1, 2) from t1 where b in (6, 7);

explain formatted
select a in (3, 4, 5), a in (1, 2) from t1 where b in (6, 7);
