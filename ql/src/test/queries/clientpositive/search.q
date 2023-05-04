create table t1 (a int, b int);

--explain cbo
--select * from t1 where a in (1);
--

set hive.optimize.transform.in.maxnodes=1000;

explain cbo
select * from t1 where a between 1 and 5 or a between 10 and 15;
explain
select * from t1 where a between 1 and 5 or a between 10 and 15;
explain cbo
select * from t1 where a between 1 and 5 or a in (10, 12, 15) or a > 100;
explain
select * from t1 where a between 1 and 5 or a in (10, 12, 15) or a > 100;
explain cbo
select * from t1 where a in (1, 2, 3);
explain
select * from t1 where a in (1, 2, 3);
explain cbo
select * from t1 where a in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22);
explain
select * from t1 where a in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22);
