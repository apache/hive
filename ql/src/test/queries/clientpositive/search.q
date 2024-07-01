set hive.cbo.returnpath.hiveop=false;

create table t1 (a int, b int);

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
select * from t1 where a in (1);
explain
select * from t1 where a in (1);
explain cbo
select * from t1 where a in (1, 2, 3);
explain
select * from t1 where a in (1, 2, 3);
explain cbo
select * from t1 where a in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22);
explain
select * from t1 where a in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22);
explain cbo
select * from t1 where a is null or a=1 or a=2;
explain
select * from t1 where a is null or a=1 or a=2;
explain cbo
select * from t1 where a is not null or a=1 or a=2;
explain
select * from t1 where a is not null or a=1 or a=2;
explain cbo
select * from t1 where a is not null and (a=1 or a=2);
explain
select * from t1 where a is not null and (a=1 or a=2);
explain cbo
select * from t1 where a between 1 and 5 or a not between 10 and 15;
explain
select * from t1 where a between 1 and 5 or a not between 10 and 15;
explain cbo
select * from t1 where a between 1 and 50 and a not between 10 and 15;
explain
select * from t1 where a between 1 and 50 and a not between 10 and 15;
explain cbo
select * from t1 where a between 1 and 5 and a not between 10 and 15;
explain
select * from t1 where a between 1 and 5 and a not between 10 and 15;
explain cbo
select * from t1 where a not between 1 and 5 and a not between 10 and 15;
explain
select * from t1 where a not between 1 and 5 and a not between 10 and 15;
explain cbo
select * from t1 where a between 1 and 5 or b not between 10 and 15;
explain
select * from t1 where a between 1 and 5 or b not between 10 and 15;
explain cbo
select * from t1 where a between 1 and 5 and b not between 10 and 15;
explain
select * from t1 where a between 1 and 5 and b not between 10 and 15;
explain cbo
select * from t1 where (a is null or a=1 or a=2) and (b is not null and b in (4, 6, 8));
explain
select * from t1 where (a is null or a=1 or a=2) and (b is not null and b in (4, 6, 8));

set hive.cbo.returnpath.hiveop=true;

explain
select * from t1 where a between 1 and 5 or a between 10 and 15;
explain
select * from t1 where a between 1 and 5 or a in (10, 12, 15) or a > 100;
explain
select * from t1 where a in (1);
explain
select * from t1 where a in (1, 2, 3);
explain
select * from t1 where a in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22);
explain
select * from t1 where a is null or a=1 or a=2;
explain
select * from t1 where a is not null or a=1 or a=2;
explain
select * from t1 where a is not null and (a=1 or a=2);
explain
select * from t1 where a between 1 and 5 or a not between 10 and 15;
explain
select * from t1 where a between 1 and 50 and a not between 10 and 15;
explain
select * from t1 where a between 1 and 5 and a not between 10 and 15;
explain
select * from t1 where a not between 1 and 5 and a not between 10 and 15;
explain
select * from t1 where a between 1 and 5 or b not between 10 and 15;
explain
select * from t1 where a between 1 and 5 and b not between 10 and 15;
explain
select * from t1 where (a is null or a=1 or a=2) and (b is not null and b in (4, 6, 8));
