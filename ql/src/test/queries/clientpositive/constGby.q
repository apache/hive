set hive.mapred.mode=nonstrict;

create table t1_n36 (a int);
analyze table t1_n36 compute statistics;
analyze table t1_n36 compute statistics for columns;

explain select count(1) from t1_n36 group by 1;
select count(1) from t1_n36 group by 1;
select count(1) from t1_n36;
explain select count(*) from t1_n36;
select count(*) from t1_n36;
select count(1) from t1_n36 group by 1=1;
select count(1), max(a) from t1_n36 group by 1=1;

set hive.compute.query.using.stats=false;

select count(1) from t1_n36 group by 1;
select count(1) from t1_n36;
select count(*) from t1_n36;
select count(1) from t1_n36 group by 1=1;
select count(1), max(a) from t1_n36 group by 1=1;
