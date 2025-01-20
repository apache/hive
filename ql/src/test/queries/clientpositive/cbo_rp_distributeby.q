set hive.cbo.returnpath.hiveop=true;

create table t1 (a string, b int);

insert into t1 values ('2014-03-14 10:10:12', 10);

-- distribute by
explain
select * from t1 where a between date_add('2014-03-14', -1) and '2014-03-14' distribute by a;
select * from t1 where a between date_add('2014-03-14', -1) and '2014-03-14' distribute by a;

-- distribute by and sort by
explain
select * from t1 distribute by a, b sort by a;

-- cluster by
explain
select * from t1 cluster by a, b;
